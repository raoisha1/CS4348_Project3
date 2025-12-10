# Devlog

## 2025-12-09 19:52

### Thoughts so far
 build a command-line B-Tree index manager that stores nodes on disk in blocks and only keeps up to 3 in memory. The commands are: create, insert, search, load, print, extract. 

plan
- Set up Python .
- Parse command-line arguments.
- Design header/node layout.