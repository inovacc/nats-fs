package natsfs

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/inovacc/utils/v2/uid"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestFS(t *testing.T) Fs {
	nc, err := nats.Connect(nats.DefaultURL)
	require.NoError(t, err)

	js, err := nc.JetStream()
	require.NoError(t, err)

	bucket := "test-fs" // fmt.Sprintf("test-%s", uid.GenerateUUID())

	fsys, err := NewNatsFs(js, Config{
		ConnectionID: uid.GenerateUUID(),
		Bucket:       bucket,
		Description:  "test fs",
		TTL:          time.Hour,
		Storage:      MemoryStorage,
	})
	require.NoError(t, err)

	// t.Cleanup(func() {
	// 	_ = js.DeleteObjectStore(bucket)
	// })

	return fsys
}

func TestCreateReadFile(t *testing.T) {
	fsys := setupTestFS(t)
	f, err := fsys.Create("product.json")
	require.NoError(t, err)

	data, err := json.Marshal(gofakeit.Product())
	require.NoError(t, err)

	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f2, err := fsys.Open("product.json")
	require.NoError(t, err)

	buf := make([]byte, len(data))
	n, err := f2.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, buf)
}

func TestRenameFile(t *testing.T) {
	fsys := setupTestFS(t)
	f, err := fsys.Create("rename.txt")
	require.NoError(t, err)
	_, _ = f.WriteString("data")
	require.NoError(t, f.Close())

	require.NoError(t, fsys.Rename("rename.txt", "newname.txt"))
	_, err = fsys.Open("rename.txt")
	assert.Error(t, err)

	f2, err := fsys.Open("newname.txt")
	require.NoError(t, err)
	buf := make([]byte, 4)
	_, _ = f2.Read(buf)
	assert.Equal(t, "data", string(buf))
}

func TestRemoveAll(t *testing.T) {
	fsys := setupTestFS(t)
	require.NoError(t, fsys.MkdirAll("dir/sub", 0755))
	f, err := fsys.Create("dir/sub/file.txt")
	require.NoError(t, err)
	_, _ = f.WriteString("to delete")
	require.NoError(t, f.Close())

	require.NoError(t, fsys.RemoveAll("dir"))
	_, err = fsys.Stat("dir/sub/file.txt")
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestLocking(t *testing.T) {
	fsys := setupTestFS(t)
	f, err := fsys.Create("lock.txt")
	require.NoError(t, err)
	_, _ = f.WriteString("locked")
	require.NoError(t, f.Sync())
}

func BenchmarkFileWriteRead(b *testing.B) {
	fsys := setupTestFS(&testing.T{})
	data := []byte("stress test content")

	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("file-%d.txt", i)
		f, err := fsys.Create(name)
		require.NoError(b, err)
		_, _ = f.Write(data)
		_ = f.Close()

		f2, err := fsys.Open(name)
		require.NoError(b, err)
		buf := make([]byte, len(data))
		_, _ = f2.Read(buf)
	}
}
