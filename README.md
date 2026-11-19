# Fatawa Razviya — Jild 22 tashreeh v4 merge

Merge workspace: 12 leaf chunk files of the easy-Urdu tashreeh v4 for Fatawa Razviya Jild 22 (volume_id 42; 180 items, 585 pages) are merged into `jild-22.json` by GitHub Actions (`merge_jild22_v4.py`), then published as a v1.0 release asset.

Item order reproduces the v3 merged file exactly (first jild22-pesh-lafz, last jild22-khatima). All 12 leaf chunks carry start_page == min(pages). No renames at merge — the two tail ids (safaih-allujayn, hawashi-zubda) are absorbed by their cross-id joins — and the merge joins 4 simple duplicate-id groups (masala-152, masala-186 [3 parts], masala-205-208, masala-227) plus 2 cross-id joins (masala-123 head + safaih-allujayn tail; masala-186 head + hawashi-zubda tail), then stable-sorts by start_page, matching the v3 merged file. No page markers are present in the chunks.
