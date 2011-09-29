#ifndef BLOCKS_HPP
#define BLOCKS_HPP

#define BOOST_FILESYSTEM_VERSION 3

#define BLOCKSIZE 8192

#include <string>
#include <vector>
#include <boost/smart_ptr.hpp>
#include <boost/filesystem.hpp>
//#include <boost/ptr_container/ptr_container.hpp>

namespace replican {

/**
 * An rsync-style weak rolling checksum.
 */
class WeakChecksum {
public:
    
    int a;
    int b;
    
    WeakChecksum();
    WeakChecksum(int _a, int _b);
    virtual ~WeakChecksum();
    
    inline int getChecksum() { return b << 16 | a; }
    
    inline void roll(char removedByte, char newByte) {
        a -= removedByte - newByte;
        b -= removedByte * BLOCKSIZE - a;
    }
    
    /**
     * Calculate the weak checksum on a buffer of data.
     */
    void update(int len, char* buf);
    
};

/*
class Node;
class Block;
class File;
class Dir;
*/

class Node;
class Block;
class File;
class Dir;

typedef boost::shared_ptr<Node> NodePtr;
typedef boost::shared_ptr<Block> BlockPtr;
typedef boost::shared_ptr<File> FilePtr;
typedef boost::shared_ptr<Dir> DirPtr;

class Node {
public:
    virtual ~Node() = 0;
    
    inline const std::string& get_strong() { return strong; }
    
    virtual const NodePtr get_parent() = 0;
    
    std::vector<NodePtr>& get_children() { return children; }
    
protected:
    std::string strong;
    std::vector<NodePtr> children;
};

class Block : public Node {
public:
    Block(const FilePtr _file, int _offset);
    virtual ~Block();
    
    inline virtual const NodePtr parent() { return NodePtr(file.get()); }
    
private:
    const FilePtr file;
    const int offset;
};

class FsNode : public Node {
public:
    FsNode(const std::string& _name);
    FsNode(const DirPtr _dir, const std::string& _name);
    virtual ~FsNode() = 0;
    
    inline virtual const NodePtr parent() { return NodePtr(dir.get()); }
    
protected:
    const DirPtr dir;
    std::string name;
};

class File : public FsNode {
public:
    File(const DirPtr _dir, const std::string& _name);
    virtual ~File();
    
};

class Dir : public FsNode {
public:
    Dir(const std::string& _name);
    Dir(const DirPtr _parent, const std::string& _name);
    virtual ~Dir();
    
};

DirPtr index(boost::filesystem::path& root_path);

/*
function M.bintohex(s)
  return (s:gsub('(.)', function(c)
    return string.format('%02x', string.byte(c))
  end))
end 

-- Start a weak checksum on a block of data
-- Good for a rolling checksum
function M.start_cksum(data)
    local a = 0
    local b = 0
    local l = data:len()
    local x
    
    for i = 1, l do
        x = data:byte(i)
        a = a + x
        b = b + (l - i) * x
    end
    
    return a, b
end

-- Complete weak checksum on a smallish block
function M.weak_cksum(data)
    local a, b = M.start_cksum(data)
    return (b * 65536) + a
end

-- Roll checksum byte-by-byte
function M.roll_cksum(removed_byte, added_byte, a, b)
    a = a - (removed_byte - added_byte)
    b = b - ((removed_byte * M.BLOCKSIZE) - a)
    return a, b
end

function M.strong_cksum(data)
    local hash = crypto.digest.new("sha1")
    hash:update(data)
    return hash:final()
end

BlockIndex = class()

function BlockIndex:_init(weak, strong)
    self.weak = weak
    self.strong = strong
end

function BlockIndex:get_hash()
    return self.strong
end

FileIndex = class()

function FileIndex:_init(name)
    self.name = name
    self.strong = nil
    self.blocks = {}
end

function FileIndex:get_hash()
    return self.strong
end

function M.get_file_index(path)
    local index = FileIndex(plpath.basename(path))
    local f, err = io.open(path, "r")
    
    if not f then
        print("cannot access '" .. path .. "': " .. err)
        return nil
    end
     
    local block_num = 1
    local buf
    local hash = crypto.digest.new("sha1")
    
    while true do
        buf = f:read(M.BLOCKSIZE)
        if not buf then
            break
        end
        
        index.blocks[block_num] = BlockIndex(M.weak_cksum(buf), M.strong_cksum(buf))
        
        hash:update(buf)
        
        block_num = block_num + 1
    end
    
    io.close(f)
    index.strong = hash:final()
    
    return index
end

function M.get_file_block(path, block_nums)
    local result = {}
    local f = io.open(path, "r")
    
    for block_num in pairs(block_nums) do
        f:seek(block_num * M.BLOCKSIZE)
        result[block_num] = f:read(M.BLOCKSIZE)
    end
    
    return result
end

DirIndex = class()

function DirIndex:_init(name)
    self.name = name
    self.dirs = {}
    self.files = {}
    self.strong = nil
end

function DirIndex:get_hash()
    local hash = crypto.digest.new("sha1")
    hash:update(tostring(self))
    return hash:final()
end

function DirIndex:finalize()
    
    local indexed_dirs = {}
    for _, dir in pairs(self.dirs) do
        indexed_dirs[dir:get_hash()] = dir:finalize()
    end
    self.dirs = indexed_dirs
    
    local indexed_files = {}
    for _, file in pairs(self.files) do
        indexed_files[file:get_hash()] = file
    end
    self.files = indexed_files
    
    self.strong = self:get_hash()
    
    return self
end

function DirIndex:__tostring()
    -- TODO: memoize this
    local acc = {}
    
    for _, dir in pairs(self.dirs) do
        table.insert(acc, dir.name)
        table.insert(acc, "\td\t")
        table.insert(acc, dir:get_hash())
        table.insert(acc, "\n")
    end
    
    for _, file in pairs(self.files) do
        table.insert(acc, file.name)
        table.insert(acc, "\tf\t")
        table.insert(acc, file:get_hash())
        table.insert(acc, "\n")
    end
    
    return table.concat(acc)
end

function M.get_dir_index(path)
    local root_index = DirIndex(plpath.basename(path))
    local path_map = {}
    path_map[path]=root_index
    local dir_index = nil
    
    for root, subdirs, files in dir.walk(path) do
        dir_index = path_map[root]
        
        local fpath
        local findex
        for i, f in pairs(files) do
            fpath = plpath.join(root, f)
            findex = M.get_file_index(fpath)
            if findex then
                table.insert(dir_index.files, findex)
            end
        end
        
        local dpath
        local sub_index
        for i, d in pairs(subdirs) do
            dpath = plpath.join(root, d)
            sub_index = DirIndex(plpath.basename(dpath))
            path_map[dpath] = sub_index
            table.insert(dir_index.dirs, sub_index)
        end
        
    end
    
    root_index:finalize()
    
    return root_index
end
*/
}

#endif
