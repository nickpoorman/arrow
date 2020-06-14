package parquet

// TODO: Implement
type FileDecryptionProperties struct{}
type FileEncryptionProperties struct{}

func (f *FileEncryptionProperties) ColumnEncryptionProperties(
	columnPath ColumnPath) *ColumnEncryptionProperties {
	panic("not yet implemented")
}

type ColumnEncryptionProperties struct{}
type ColumnDecryptionProperties struct{}
