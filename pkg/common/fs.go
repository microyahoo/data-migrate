package common

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
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
