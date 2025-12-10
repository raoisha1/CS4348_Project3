# Devlog

## 2025-12-09 19:52

### Thoughts so far
 build a command-line B-Tree index manager that stores nodes on disk in blocks and only keeps up to 3 in memory. The commands are: create, insert, search, load, print, extract. 

plan
- Set up Python .
- Parse command-line arguments.
- Design header/node layout.

## 2025-12-09 20:15
Thoughts:
- Ready to start Python i.

Plan:
- Add project3.py with CLI for commands.
- Implement I/O
- Implement node structure

# 2025-12-08 19:00
Reflection:
- Header and Node classes completed and tested for correct block size.
Next:
- Implement B-Tree insert logic.

---

## 2025-12-09 12:00
Thoughts:
- Insert requires split logic and node cache.

Plan:
- Add NodeCache with 3-node LRU.
- Add insert and split_child.

## 2025-12-09 14:00
Reflection:
- Insert working for simple cases.
- Cache eviction works.
Next:
- Test splitting with many inserts.

---

## 2025-12-09 17:00
Thoughts:
- Splits not promoting correctly in some cases.

Plan:
- Rewrite split_child.
- Retest inserts 1–25.

## 2025-12-09 17:45
Reflection:
- split_child fixed.
- insert/search/print correct for large sets.
Next:
- Implement load and extract.

---

## 2025-12-09 19:00
Thoughts:
- Need CSV loading and extract file creation.

Plan:
- Add load and extract.
- Add error for existing output file.

## 2025-12-09 19:40
Reflection:
- load and extract working.
- extract blocks overwriting correctly.
Next:
- Final test suite.

---

## 2025-12-09 22:00
Thoughts:
- Run full tests.

Plan:
- create/insert/search/print/extract
- load from CSV
- heavy insert test (1..60)

## 2025-12-09 22:30
Reflection:
- All tests passed.
- B-Tree splits and traversal correct.
- Project complete.