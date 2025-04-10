package fileutil

import (
	"os"
	"path/filepath"
)

// FileObject file object encapsulation
type FileObject struct {
	File *os.File
	Size uint32
}

func (f *FileObject) Read(offset uint32, length uint32) ([]byte, error) {
	data := make([]byte, length)
	// read data from a specified offset
	_, err := f.File.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}
	return data, nil
}

func CreateFileObject(path string, data []byte) (*FileObject, error) {
	// 确保目录存在
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// 创建并写入文件
	err := os.WriteFile(path, data, 0644)
	if err != nil {
		return nil, err
	}

	// 打开文件用于读取
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &FileObject{
		File: file,
		Size: uint32(len(data)),
	}, nil
}

func OpenFileObject(path string) (*FileObject, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	return &FileObject{
		File: file,
		Size: uint32(info.Size()),
	}, nil
}
