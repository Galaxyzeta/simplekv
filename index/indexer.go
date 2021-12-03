package index

type INode struct {
	Offset int64
	FileID string
}

type Indexer interface {
	Write(key string, inode INode)
	Read(key string) (INode, bool)
	Delete(key string)
}

func MakeInode(offset int64, fileID string) INode {
	return INode{
		Offset: offset,
		FileID: fileID,
	}
}
