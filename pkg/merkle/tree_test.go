package merkle

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMerkleTree_UpdateBucket(t *testing.T) {
	tree, err := NewMerkleTree(4)
	assert.NoError(t, err)
	assert.Equal(t, 4, tree.NumLeaves())
	assert.Equal(t, 7, len(tree.nodes)) // 2*4 - 1

	// Initially all empty
	assert.Equal(t, "", tree.GetRoot())

	// Update leaf 0
	h0 := "hash0"
	err = tree.UpdateBucket(0, h0)
	assert.NoError(t, err)

	root1 := tree.GetRoot()
	assert.NotEmpty(t, root1)

	// Update leaf 1
	h1 := "hash1"
	err = tree.UpdateBucket(1, h1)
	assert.NoError(t, err)

	root2 := tree.GetRoot()
	assert.NotEqual(t, root1, root2)

	// Children of root
	left, right, err := tree.GetChildren(0)
	assert.NoError(t, err)
	assert.NotEmpty(t, left)
	assert.Equal(t, "", right) // Bucket 2 and 3 are empty

	// Children of left
	leftLeft, leftRight, err := tree.GetChildren(1)
	assert.NoError(t, err)
	assert.Equal(t, h0, leftLeft)
	assert.Equal(t, h1, leftRight)
}

func TestMerkleTree_PowerOfTwo(t *testing.T) {
	_, err := NewMerkleTree(3)
	assert.Error(t, err)

	_, err = NewMerkleTree(1024)
	assert.NoError(t, err)
}

func TestMerkleTree_Persistence(t *testing.T) {
	tree, _ := NewMerkleTree(4)
	_ = tree.UpdateBucket(0, "a")
	_ = tree.UpdateBucket(3, "b")
	root := tree.GetRoot()

	state := tree.ExportState()

	tree2, _ := NewMerkleTree(4)
	err := tree2.ImportState(state)
	assert.NoError(t, err)
	assert.Equal(t, root, tree2.GetRoot())
}
