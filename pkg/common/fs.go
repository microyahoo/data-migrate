package common

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"

	log "github.com/sirupsen/logrus"
)

func GetFilesystemType(path string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path: %v", err)
	}

	if _, err := os.Stat(absPath); err != nil {
		return "", fmt.Errorf("path does not exist: %v", err)
	}

	switch runtime.GOOS {
	case "linux":
		return getFilesystemTypeLinux(absPath)
	default:
		return "", fmt.Errorf("unsupported operating system: %s", runtime.GOOS)
	}
}

func getFilesystemTypeLinux(path string) (string, error) {
	var stat syscall.Statfs_t

	err := syscall.Statfs(path, &stat)
	if err != nil {
		return "", fmt.Errorf("failed to statfs: %v", err)
	}
	fsTypeMap := map[int64]string{
		0xef53:     "ext4", // EXT4_SUPER_MAGIC
		0xef51:     "ext2", // EXT2_SUPER_MAGIC old
		0xef52:     "ext3", // EXT3_SUPER_MAGIC
		0x794c7630: "overlayfs",
		0x6969:     "nfs",
		0x564c:     "nfsd",
		0x5346544e: "ntfs",
		0x9fa0:     "proc",
		0x61756673: "aufs",
		0x01021994: "tmpfs",
		0x517B:     "smb",
		0x2fc12fc1: "zfs",
		0x58465342: "xfs",
		0x9fa1:     "sysfs",
		0xbd00bd0:  "unionfs",
		0x31384:    "yrfs_ec", // YRFS_SUPER_MAGIC
		0x47504653: "gpfs",    // GPFS_SUPER_MAGIC
	}

	fsType := int64(stat.Type)

	if name, exists := fsTypeMap[fsType]; exists {
		return name, nil
	}

	return fmt.Sprintf("unknown (0x%x)", fsType), nil
}

// /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_1/ /mnt/yrfs/public-data/training/samples/camera_1/ /mnt/core-data/data/3d_object_gt/data_sync/results/sync_3d/mnt#csi-data-gfs#lidar#deeproute_all#samples#camera_1#.txt
// baseDir: /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_1/
// subdirsFile: /mnt/core-data/data/3d_object_gt/data_sync/results/sync_3d/mnt#csi-data-gfs#lidar#deeproute_all#samples#camera_1#.txt
//
// FindFiles finds files and returns output file channel in real-time
// baseDir: base directory
// subdirsFile: file containing list of subdirectories
// concurrency: number of concurrent workers
// outputPrefix: prefix for output files
// maxFilesPerOutput: maximum files per output file
// Returns: output file channel and error
func FindFiles(baseDir string, subdirsFile string, concurrency int, outputDir, outputPrefix string, maxFilesPerOutput int) (<-chan string, error) {
	// Open subdirectories file
	f, err := os.Open(subdirsFile)
	if err != nil {
		return nil, err
	}

	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
		if concurrency > 128 {
			concurrency = 128
		}
	}

	if maxFilesPerOutput <= 0 {
		maxFilesPerOutput = 500000
	}

	// Create output file channel
	outputFileChan := make(chan string, 10)

	// Start a goroutine to perform the actual file finding
	go func() {
		defer f.Close()

		outputMgr := &OutputManager{
			dir:            outputDir,
			prefix:         outputPrefix,
			maxFiles:       maxFilesPerOutput,
			fileNum:        1,
			outputFileChan: outputFileChan, // Pass channel to OutputManager
		}
		defer outputMgr.Close()
		defer close(outputFileChan) // Ensure channel is closed

		subdirCh := make(chan string, concurrency*2)
		wg := &sync.WaitGroup{}

		// Start worker goroutines
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				for subdir := range subdirCh {
					root := filepath.Join(baseDir, subdir)
					err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
						if err != nil {
							log.Warningf("Warning: cannot access %s: %v", path, err)
							return nil
						}

						// Skip directories and symlinks
						if d.Type()&os.ModeSymlink != 0 || d.IsDir() {
							return nil
						}

						rel, err := filepath.Rel(baseDir, path)
						if err != nil {
							return nil
						}

						// Write to output file
						if err := outputMgr.WriteLine(rel); err != nil {
							return err
						}
						return nil
					})

					if err != nil {
						log.Errorf("error walking directory %s: %v", root, err)
					}
				}
			}(i)
		}

		// Read subdirectories list and distribute to workers
		scanner := bufio.NewScanner(f)
		dirCount := 0
		for scanner.Scan() {
			line := scanner.Text()
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			subdirCh <- line
			dirCount++

			if dirCount%10000 == 0 {
				log.Infof("Queued %d directories...", dirCount)
			}
		}

		if err := scanner.Err(); err != nil {
			log.Errorf("failed to read subdirs file: %v", err)
		}

		close(subdirCh)
		wg.Wait()

		// Process the last output file (if it has data)
		outputMgr.finalizeCurrentFile()

		totalFiles, fileCount := outputMgr.GetStats()
		log.Infof("Export completed: %d files in %d output files", totalFiles, fileCount)
	}()

	return outputFileChan, nil
}

type OutputManager struct {
	dir             string
	prefix          string
	maxFiles        int
	currentFile     *os.File
	writer          *bufio.Writer
	fileCount       int // file count per output file
	totalFiles      int
	fileNum         int           // the number of output file
	outputFileChan  chan<- string // output file name channel
	currentFileName string        // current output file name
	mu              sync.Mutex
}

func (om *OutputManager) WriteLine(line string) error {
	om.mu.Lock()
	defer om.mu.Unlock()

	// If we need to create a new file, send the previous completed file first (if exists)
	if om.currentFile != nil && om.fileCount >= om.maxFiles {
		if err := om.sendCompletedFile(); err != nil {
			return err
		}
	}

	// Create a new file if needed
	if om.currentFile == nil {
		if err := om.rotateFile(); err != nil {
			return err
		}
	}

	if _, err := om.writer.WriteString(line + "\n"); err != nil {
		return err
	}

	om.fileCount++
	om.totalFiles++

	if om.totalFiles%100000 == 0 {
		log.Infof("Processed %d files...", om.totalFiles)
	}

	return nil
}

// sendCompletedFile sends the completed file to the channel
func (om *OutputManager) sendCompletedFile() error {
	if om.currentFile == nil || om.fileCount == 0 {
		return nil // No file to send
	}

	// Flush buffer and close current file
	if err := om.writer.Flush(); err != nil {
		return err
	}
	om.currentFile.Close()

	// Send file name to channel
	if om.outputFileChan != nil && om.currentFileName != "" {
		select {
		case om.outputFileChan <- om.currentFileName:
			log.Infof("Sent completed output file to channel: %s", om.currentFileName)
			// default:
			// 	log.Warningf("Output file channel is full, could not send file: %s", om.currentFileName)
		}
	}

	// Reset counters
	om.fileCount = 0
	om.currentFile = nil
	om.currentFileName = ""
	om.fileNum++

	return nil
}

func (om *OutputManager) rotateFile() error {
	// Create a new file
	fileName := fmt.Sprintf("%s_%04d.index", om.prefix, om.fileNum)
	file, err := os.Create(filepath.Join(om.dir, fileName))
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %v", fileName, err)
	}

	om.currentFile = file
	om.writer = bufio.NewWriterSize(file, 16*1024*1024) // 16MB buffer
	om.currentFileName = fileName

	log.Infof("Created output file: %s", fileName)

	return nil
}

// finalizeCurrentFile processes the current file (if it has data)
func (om *OutputManager) finalizeCurrentFile() {
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.currentFile != nil && om.fileCount > 0 {
		if err := om.writer.Flush(); err != nil {
			log.Errorf("Failed to flush writer: %v", err)
		}
		om.currentFile.Close()

		// Send the last file
		if om.outputFileChan != nil && om.currentFileName != "" {
			select {
			case om.outputFileChan <- om.currentFileName:
				log.Infof("Sent final output file to channel: %s", om.currentFileName)
				// default:
				// 	log.Warningf("Output file channel is full, could not send final file: %s", om.currentFileName)
			}
		}
	}
}

func (om *OutputManager) Close() {
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.currentFile != nil {
		om.writer.Flush()
		om.currentFile.Close()
	}
}

func (om *OutputManager) GetStats() (totalFiles, fileCount int) {
	om.mu.Lock()
	defer om.mu.Unlock()

	// Note: fileNum is the number for the next file, so current file count is fileNum-1
	// But if we have a current file, we need to include it
	completedFiles := om.fileNum - 1
	if om.currentFile != nil && om.fileCount > 0 {
		completedFiles++
	}

	return om.totalFiles, completedFiles
}
