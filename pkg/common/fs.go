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
// targetDir: target directory
// subdirsFile: file containing list of subdirectories (if empty, will scan baseDir directly)
// concurrency: number of concurrent workers
// outputPrefix: prefix for output files
// maxFilesPerOutput: maximum files per output file
// Returns: output file channel and error
func FindFiles(baseDir, targetDir, subdirsFile string, concurrency int, outputDir, outputPrefix string, maxFilesPerOutput int) (<-chan string, error) {
	var (
		fileEntryList []os.DirEntry
		err           error
	)
	// Handle empty subdirsFile case
	if subdirsFile == "" {
		subdirsFile, fileEntryList, err = FindFilesInBaseDir(baseDir)
	}

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
	go func(entries []os.DirEntry) {
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
					if e := os.Mkdir(filepath.Join(targetDir, subdir), 0755); e != nil && !os.IsExist(e) {
						log.Warningf("failed to mkdir %s: %s", filepath.Join(targetDir, subdir), e)
					}
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
						// TODO: error
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
			// TODO: error
			log.Errorf("failed to read subdirs file: %v", err)
		}

		close(subdirCh)
		wg.Wait()

		for _, e := range entries {
			// Skip directories and symlinks
			if e.Type()&os.ModeSymlink != 0 {
				continue
			}

			// Write to output file
			if err := outputMgr.WriteLine(e.Name()); err != nil {
				// TODO: error
				log.Errorf("failed to write line: %s", err)
			}
		}

		// Process the last output file (if it has data)
		outputMgr.finalizeCurrentFile()

		totalFiles, fileCount := outputMgr.GetStats()
		log.Infof("Export completed: %d files in %d output files", totalFiles, fileCount)
	}(fileEntryList)

	return outputFileChan, nil
}

// FindFilesInBaseDir finds all files in base directory and its subdirectories
// It creates two files: one for files directly in baseDir, and another for subdirectories
// Then it uses FindFiles with the subdirectories file
// Returns: subdirs file, file entry list and error
func FindFilesInBaseDir(baseDir string) (string, []os.DirEntry, error) {
	// Create temporary directory for intermediate files
	tempDir, err := os.MkdirTemp("", "findfiles_*")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temp directory: %v", err)
	}

	// Create files for different types of paths
	filesInBaseDirFile := filepath.Join(tempDir, "files_in_base_dir.txt")
	subdirsFile := filepath.Join(tempDir, "subdirs.txt")

	// Open files for writing
	filesWriter, err := os.Create(filesInBaseDirFile)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create files list: %v", err)
	}
	defer filesWriter.Close()

	subdirsWriter, err := os.Create(subdirsFile)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create subdirs list: %v", err)
	}
	defer subdirsWriter.Close()

	// Scan base directory
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return "", nil, fmt.Errorf("failed to read base directory: %v", err)
	}

	var fileEntries []os.DirEntry
	var dirEntries []os.DirEntry

	// Separate files and directories
	for _, entry := range entries {
		if entry.IsDir() {
			dirEntries = append(dirEntries, entry)
		} else if entry.Type().IsRegular() {
			fileEntries = append(fileEntries, entry)
		}
	}

	log.Infof("Found %d files directly in base directory", len(fileEntries))
	log.Infof("Found %d subdirectories in base directory", len(dirEntries))

	// Write files directly in baseDir to filesInBaseDirFile
	for _, file := range fileEntries {
		if _, err := filesWriter.WriteString(file.Name() + "\n"); err != nil {
			return "", nil, fmt.Errorf("failed to write file entry: %v", err)
		}
	}

	// Write subdirectories to subdirsFile
	for _, dir := range dirEntries {
		if _, err := subdirsWriter.WriteString(dir.Name() + "\n"); err != nil {
			return "", nil, fmt.Errorf("failed to write directory entry: %v", err)
		}
	}

	// Flush files
	if err := filesWriter.Sync(); err != nil {
		log.Warningf("Failed to sync files list: %v", err)
		return "", nil, err
	}

	if err := subdirsWriter.Sync(); err != nil {
		log.Warningf("Failed to sync subdirs list: %v", err)
		return "", nil, err
	}

	return subdirsFile, fileEntries, nil
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
	om.currentFileName = file.Name()

	log.Infof("Created output file: %s", file.Name())

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
