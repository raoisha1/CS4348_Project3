import sys
import os
import csv

BLOCK_SIZE = 512
MAGIC = b"4348PRJ3"
T = 10
MAX_KEYS = 2 * T - 1
MAX_CHILDREN = 2 * T


class Header:
    def __init__(self):
        self.magic = MAGIC
        self.root_block = 0
        self.next_block = 1

    @staticmethod
    def from_file(f):
        f.seek(0)
        data = f.read(BLOCK_SIZE)
        if len(data) != BLOCK_SIZE:
            raise ValueError("Failed to read header")
        magic = data[0:8]
        if magic != MAGIC:
            raise ValueError("Invalid magic")
        root_block = int.from_bytes(data[8:16], "big", signed=False)
        next_block = int.from_bytes(data[16:24], "big", signed=False)
        h = Header()
        h.magic = magic
        h.root_block = root_block
        h.next_block = next_block
        return h

    def to_bytes(self):
        buf = bytearray(BLOCK_SIZE)
        buf[0:8] = self.magic
        buf[8:16] = self.root_block.to_bytes(8, "big", signed=False)
        buf[16:24] = self.next_block.to_bytes(8, "big", signed=False)
        return bytes(buf)

    def write(self, f):
        f.seek(0)
        f.write(self.to_bytes())
        f.flush()


class Node:
    def __init__(self, block_id=0, parent_id=0):
        self.block_id = block_id
        self.parent_id = parent_id
        self.num_keys = 0
        self.keys = [0] * MAX_KEYS
        self.values = [0] * MAX_KEYS
        self.children = [0] * MAX_CHILDREN
        self.dirty = True

    @staticmethod
    def read(f, block_id):
        f.seek(block_id * BLOCK_SIZE)
        data = f.read(BLOCK_SIZE)
        if len(data) != BLOCK_SIZE:
            raise ValueError("Failed to read node")
        offset = 0
        stored_block_id = int.from_bytes(data[offset:offset + 8], "big", signed=False)
        offset += 8
        parent_id = int.from_bytes(data[offset:offset + 8], "big", signed=False)
        offset += 8
        num_keys = int.from_bytes(data[offset:offset + 8], "big", signed=False)
        offset += 8
        keys = []
        for _ in range(MAX_KEYS):
            k = int.from_bytes(data[offset:offset + 8], "big", signed=False)
            keys.append(k)
            offset += 8
        values = []
        for _ in range(MAX_KEYS):
            v = int.from_bytes(data[offset:offset + 8], "big", signed=False)
            values.append(v)
            offset += 8
        children = []
        for _ in range(MAX_CHILDREN):
            c = int.from_bytes(data[offset:offset + 8], "big", signed=False)
            children.append(c)
            offset += 8
        n = Node(block_id=stored_block_id, parent_id=parent_id)
        n.num_keys = num_keys
        n.keys = keys
        n.values = values
        n.children = children
        n.dirty = False
        return n

    def to_bytes(self):
        buf = bytearray(BLOCK_SIZE)
        offset = 0
        buf[offset:offset + 8] = self.block_id.to_bytes(8, "big", signed=False)
        offset += 8
        buf[offset:offset + 8] = self.parent_id.to_bytes(8, "big", signed=False)
        offset += 8
        buf[offset:offset + 8] = self.num_keys.to_bytes(8, "big", signed=False)
        offset += 8
        for i in range(MAX_KEYS):
            buf[offset:offset + 8] = self.keys[i].to_bytes(8, "big", signed=False)
            offset += 8
        for i in range(MAX_KEYS):
            buf[offset:offset + 8] = self.values[i].to_bytes(8, "big", signed=False)
            offset += 8
        for i in range(MAX_CHILDREN):
            buf[offset:offset + 8] = self.children[i].to_bytes(8, "big", signed=False)
            offset += 8
        return bytes(buf)

    def write(self, f):
        f.seek(self.block_id * BLOCK_SIZE)
        f.write(self.to_bytes())
        f.flush()
        self.dirty = False

    def is_leaf(self):
        for i in range(self.num_keys + 1):
            if self.children[i] != 0:
                return False
        return True


class NodeCache:
    def __init__(self, f):
        self.f = f
        self.cache = {}
        self.order = []

    def _touch(self, block_id):
        if block_id in self.order:
            self.order.remove(block_id)
        self.order.insert(0, block_id)

    def _evict_if_needed(self):
        if len(self.cache) <= 3:
            return
        while len(self.cache) > 3:
            victim = self.order.pop()
            node = self.cache.pop(victim)
            if node.dirty:
                node.write(self.f)

    def get(self, block_id):
        if block_id in self.cache:
            node = self.cache[block_id]
            self._touch(block_id)
            return node
        if len(self.cache) >= 3:
            self._evict_if_needed()
        node = Node.read(self.f, block_id)
        self.cache[block_id] = node
        self._touch(block_id)
        return node

    def new_node(self, block_id, parent_id):
        if len(self.cache) >= 3:
            self._evict_if_needed()
        node = Node(block_id=block_id, parent_id=parent_id)
        self.cache[block_id] = node
        self._touch(block_id)
        return node

    def flush_all(self):
        for node in self.cache.values():
            if node.dirty:
                node.write(self.f)


class BTree:
    def __init__(self, filename, must_exist):
        self.filename = filename
        mode = "r+b" if must_exist else "w+b"
        if must_exist and not os.path.exists(filename):
            raise ValueError("Index file does not exist")
        if not must_exist and os.path.exists(filename):
            raise ValueError("File already exists")
        self.f = open(filename, mode)
        if must_exist:
            self.header = Header.from_file(self.f)
        else:
            self.header = Header()
            self.header.write(self.f)
        self.cache = NodeCache(self.f)

    def close(self):
        self.cache.flush_all()
        self.header.write(self.f)
        self.f.close()

    def search(self, key):
        if self.header.root_block == 0:
            return None
        return self._search_node(self.header.root_block, key)

    def _search_node(self, block_id, key):
        node = self.cache.get(block_id)
        i = 0
        while i < node.num_keys and key > node.keys[i]:
            i += 1
        if i < node.num_keys and key == node.keys[i]:
            return node.values[i]
        if node.is_leaf():
            return None
        child_id = node.children[i]
        if child_id == 0:
            return None
        return self._search_node(child_id, key)

    def insert(self, key, value):
        if self.header.root_block == 0:
            root_id = self.header.next_block
            self.header.next_block += 1
            self.header.root_block = root_id
            root = self.cache.new_node(root_id, 0)
            root.num_keys = 1
            root.keys[0] = key
            root.values[0] = value
            root.dirty = True
            return
        root = self.cache.get(self.header.root_block)
        if root.num_keys == MAX_KEYS:
            new_root_id = self.header.next_block
            self.header.next_block += 1
            new_root = self.cache.new_node(new_root_id, 0)
            new_root.num_keys = 0
            new_root.children[0] = root.block_id
            root.parent_id = new_root_id
            root.dirty = True
            self.header.root_block = new_root_id
            self._split_child(new_root, 0)
            self._insert_nonfull(new_root, key, value)
        else:
            self._insert_nonfull(root, key, value)

    def _split_child(self, parent, i):
        old_child_id = parent.children[i]
        old_child = self.cache.get(old_child_id)
        new_child_id = self.header.next_block
        self.header.next_block += 1
        new_child = self.cache.new_node(new_child_id, parent.block_id)
        mid = T - 1
        new_child.num_keys = MAX_KEYS - T
        for j in range(new_child.num_keys):
            new_child.keys[j] = old_child.keys[j + T]
            new_child.values[j] = old_child.values[j + T]
        if not old_child.is_leaf():
            for j in range(MAX_KEYS - T + 1):
                new_child.children[j] = old_child.children[j + T]
        for j in range(mid, MAX_KEYS):
            if j >= mid:
                old_child.keys[j] = 0
                old_child.values[j] = 0
        if not old_child.is_leaf():
            for j in range(T, MAX_CHILDREN):
                old_child.children[j] = 0
        old_child.num_keys = mid
        for j in range(parent.num_keys, i, -1):
            parent.children[j + 1] = parent.children[j]
        parent.children[i + 1] = new_child_id
        for j in range(parent.num_keys - 1, i - 1, -1):
            parent.keys[j + 1] = parent.keys[j]
            parent.values[j + 1] = parent.values[j]
        parent.keys[i] = old_child.keys[mid]
        parent.values[i] = old_child.values[mid]
        old_child.keys[mid] = 0
        old_child.values[mid] = 0
        parent.num_keys += 1
        parent.dirty = True
        old_child.dirty = True
        new_child.dirty = True

    def _insert_nonfull(self, node, key, value):
        i = node.num_keys - 1
        if node.is_leaf():
            while i >= 0 and key < node.keys[i]:
                node.keys[i + 1] = node.keys[i]
                node.values[i + 1] = node.values[i]
                i -= 1
            node.keys[i + 1] = key
            node.values[i + 1] = value
            node.num_keys += 1
            node.dirty = True
        else:
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            child_id = node.children[i]
            child = self.cache.get(child_id)
            if child.num_keys == MAX_KEYS:
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1
                child_id = node.children[i]
                child = self.cache.get(child_id)
            self._insert_nonfull(child, key, value)

    def traverse(self, func):
        if self.header.root_block == 0:
            return
        self._traverse_node(self.header.root_block, func)

    def _traverse_node(self, block_id, func):
        node = self.cache.get(block_id)
        for i in range(node.num_keys):
            if node.children[i] != 0:
                self._traverse_node(node.children[i], func)
            func(node.keys[i], node.values[i])
        if node.children[node.num_keys] != 0:
            self._traverse_node(node.children[node.num_keys], func)


def cmd_create(index_file):
    if os.path.exists(index_file):
        print("Error: file exists")
        sys.exit(1)
    tree = BTree(index_file, must_exist=False)
    tree.close()


def cmd_insert(index_file, key_str, value_str):
    tree = BTree(index_file, must_exist=True)
    try:
        key = int(key_str)
        value = int(value_str)
    except ValueError:
        print("Error: key and value must be integers")
        tree.close()
        sys.exit(1)
    tree.insert(key, value)
    tree.close()


def cmd_search(index_file, key_str):
    tree = BTree(index_file, must_exist=True)
    try:
        key = int(key_str)
    except ValueError:
        print("Error: key must be integer")
        tree.close()
        sys.exit(1)
    res = tree.search(key)
    if res is None:
        print("Error: key not found")
    else:
        print(f"{key} {res}")
    tree.close()


def cmd_load(index_file, csv_file):
    if not os.path.exists(csv_file):
        print("Error: CSV file does not exist")
        sys.exit(1)
    tree = BTree(index_file, must_exist=True)
    with open(csv_file, newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 2:
                continue
            try:
                key = int(row[0].strip())
                value = int(row[1].strip())
            except ValueError:
                continue
            tree.insert(key, value)
    tree.close()


def cmd_print(index_file):
    tree = BTree(index_file, must_exist=True)

    def out(k, v):
        print(f"{k} {v}")

    tree.traverse(out)
    tree.close()


def cmd_extract(index_file, out_file):
    if os.path.exists(out_file):
        print("Error: output file exists")
        sys.exit(1)
    tree = BTree(index_file, must_exist=True)
    with open(out_file, "w", newline="") as f:
        writer = csv.writer(f)

        def write_pair(k, v):
            writer.writerow([k, v])

        tree.traverse(write_pair)
    tree.close()


def main():
    if len(sys.argv) < 3:
        print("usage: project3 <command> <args>")
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == "create":
        if len(sys.argv) != 3:
            print("usage: project3 create <index>")
            sys.exit(1)
        cmd_create(sys.argv[2])
    elif cmd == "insert":
        if len(sys.argv) != 5:
            print("usage: project3 insert <index> <key> <value>")
            sys.exit(1)
        cmd_insert(sys.argv[2], sys.argv[3], sys.argv[4])
    elif cmd == "search":
        if len(sys.argv) != 4:
            print("usage: project3 search <index> <key>")
            sys.exit(1)
        cmd_search(sys.argv[2], sys.argv[3])
    elif cmd == "load":
        if len(sys.argv) != 4:
            print("usage: project3 load <index> <csv>")
            sys.exit(1)
        cmd_load(sys.argv[2], sys.argv[3])
    elif cmd == "print":
        if len(sys.argv) != 3:
            print("usage: project3 print <index>")
            sys.exit(1)
        cmd_print(sys.argv[2])
    elif cmd == "extract":
        if len(sys.argv) != 4:
            print("usage: project3 extract <index> <csv>")
            sys.exit(1)
        cmd_extract(sys.argv[2], sys.argv[3])
    else:
        print("Error: unknown command")
        sys.exit(1)


if __name__ == "__main__":
    main()