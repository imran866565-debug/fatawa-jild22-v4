#!/usr/bin/env python3
"""Merge Fatawa Razviya Jild 22 easy-Urdu tashreeh v4 chunks into jild-22.json.

Reads the 12 leaf chunk files listed in chunks-manifest-v4-jild22.json,
concatenates them in order 01 -> 02 -> ... -> 12, JOINS the 4 simple
duplicate-id groups (masala-152, masala-186 [3 parts], masala-205-208,
masala-227) and the 2 cross-id groups (masala-123: head +
safaih-allujayn tail; masala-186: 3-part-joined head + hawashi-zubda
tail) into single items, STABLE SORTS by start_page (mirroring the v3
merge), derives start_page = min(pages) where the field is absent (0
derived: all chunks carry start_page), normalises page markers,
validates, and writes {"masail": [...]} to jild-22.json.

ORDER -- chunk order is the order, then a stable sort by start_page:
  The v3 merged file ~/workspace/fatawa-app/content/tashreeh/jild-22.json
  has ID order == stable sort by start_page of the concatenation
  (verified locally by the coordinator: the v3 order IS the stable
  sort, including its tied start_pages). Do NOT sort or reorder before
  the joins. Within each chunk, items keep file order.
  CHUNK_ORDER is an explicit 12-leaf list.

JOINS: parts are found BY ID anywhere in the list. Parts must be in
  ascending page order. Sawal modes: "first" (152, 186, 205-208, 227
  — the single non-empty sawal, in the first part), "head_only"
  (123, 186-cross — the head's sawal; the tails have empty sawals).
  Titles: the joined title is the FIRST/HEAD part's title, asserted
  against the embedded merged-v3 title (V3_TITLES). title_must_match
  is deliberately NOT used: the two halves' titles differ by design
  in the v3 chunks.
  masala-152 [382-390, 391-400], masala-186 [425-440, 441-490,
  491-540], masala-205-208 [580-590, 591-592], masala-227 [607-640,
  641-677].
CROSS-ID JOINS (2):
  masala-123: c04 head jild22-masala-123 [269-300] (sawal) + c05
    jild22-safaih-allujayn [301-314] (sawal empty) -> ONE item
    jild22-masala-123; title from the HEAD (== merged-v3 title);
    the rename is effected by the combine. Net -1 item.
  masala-186: the 3-part-joined head jild22-masala-186 [425-540] +
    c10 jild22-hawashi-zubda [541-542] (sawal empty) -> ONE item
    jild22-masala-186 [425-542], 118 pages; title from the HEAD (==
    merged-v3 title, 'مسئلہ ۱۸۶'); the rename is effected by the
    combine. Net -1 item (on top of the -2 from the consecutive join).
  After the joins there are 180 items and 180 unique ids — the
  duplicate-id assertion runs AFTER the joins, not before.
  NO renames at merge: the two tail ids are absorbed by their
  cross-id joins; no print-error renames were applied in the leaves.
  All chunks carry start_page == min(pages) (0 derived). No page sits
  in two leaves.
"""
import json
import os
import re

BASE = os.path.dirname(os.path.abspath(__file__))
MANIFEST = "chunks-manifest-v4-jild22.json"

# Concatenation order: chunk files 01 -> 18, in order.
CHUNK_ORDER = ["jild-22-chunk-01.json", "jild-22-chunk-02.json", "jild-22-chunk-03.json", "jild-22-chunk-04.json", "jild-22-chunk-05.json", "jild-22-chunk-06.json", "jild-22-chunk-07.json", "jild-22-chunk-08.json", "jild-22-chunk-09.json", "jild-22-chunk-10.json", "jild-22-chunk-11.json", "jild-22-chunk-12.json"]

EXPECTED_ITEMS = 180
EXPECTED_PAGES = 585
EXPECTED_MIN_PAGE = 5
EXPECTED_MAX_PAGE = 692
EXPECTED_VOLUME_IDS = {42}
EXPECTED_FIRST_ID = "jild22-pesh-lafz"
EXPECTED_LAST_ID = "jild22-khatima"

# v3 ID sequence, verbatim from content/tashreeh/jild-22.json.
V3_IDS = ["jild22-pesh-lafz", "jild22-masala-1", "jild22-masala-2", "jild22-masala-3", "jild22-masala-4", "jild22-masala-5", "jild22-masala-6-7", "jild22-masala-8-9", "jild22-masala-10", "jild22-masala-11-12", "jild22-masala-13-14", "jild22-masala-15-16", "jild22-masala-17", "jild22-masala-18", "jild22-masala-19", "jild22-masala-20", "jild22-masala-21", "jild22-masala-22", "jild22-masala-23-24", "jild22-masala-25", "jild22-masala-26", "jild22-masala-27", "jild22-masala-28", "jild22-masala-29-30", "jild22-masala-31", "jild22-masala-32", "jild22-masala-33", "jild22-masala-34", "jild22-masala-35", "jild22-masala-36", "jild22-masala-37", "jild22-masala-38", "jild22-masala-39", "jild22-masala-40", "jild22-masala-41", "jild22-masala-42", "jild22-masala-43", "jild22-masala-44", "jild22-masala-45", "jild22-masala-46-47", "jild22-masala-48", "jild22-masala-49", "jild22-masala-50", "jild22-masala-51", "jild22-masala-52", "jild22-masala-53", "jild22-masala-54", "jild22-masala-55", "jild22-masala-56", "jild22-masala-57", "jild22-masala-58", "jild22-masala-59", "jild22-masala-60", "jild22-masala-61", "jild22-masala-62", "jild22-masala-63", "jild22-masala-64", "jild22-masala-65", "jild22-masala-66", "jild22-masala-67-71", "jild22-masala-72-87", "jild22-masala-88", "jild22-masala-89", "jild22-masala-90", "jild22-masala-91", "jild22-masala-92", "jild22-masala-93", "jild22-masala-94", "jild22-masala-95", "jild22-masala-96", "jild22-masala-97", "jild22-masala-98", "jild22-masala-99", "jild22-masala-100", "jild22-masala-101-102", "jild22-masala-103", "jild22-masala-104", "jild22-masala-105-106", "jild22-masala-107", "jild22-masala-108", "jild22-masala-109-110", "jild22-masala-111", "jild22-masala-112", "jild22-masala-113", "jild22-masala-114", "jild22-masala-115", "jild22-masala-116", "jild22-masala-117", "jild22-masala-118", "jild22-masala-119-120", "jild22-masala-121", "jild22-masala-122", "jild22-masala-123", "jild22-masala-124", "jild22-masala-125", "jild22-masala-126", "jild22-masala-127", "jild22-masala-128", "jild22-masala-129", "jild22-masala-130", "jild22-masala-131", "jild22-masala-132", "jild22-masala-133", "jild22-masala-134", "jild22-masala-135-139", "jild22-masala-140", "jild22-masala-141", "jild22-masala-142", "jild22-masala-143", "jild22-masala-144", "jild22-masala-145", "jild22-masala-146", "jild22-masala-147-148", "jild22-masala-149", "jild22-masala-150", "jild22-masala-151", "jild22-masala-152", "jild22-masala-153-154", "jild22-masala-155", "jild22-masala-156", "jild22-masala-157", "jild22-masala-158-163", "jild22-masala-164-168", "jild22-masala-169", "jild22-masala-170-171", "jild22-masala-172", "jild22-masala-173", "jild22-masala-174", "jild22-masala-175", "jild22-masala-176", "jild22-masala-177", "jild22-masala-178", "jild22-masala-179", "jild22-masala-180-183", "jild22-masala-184", "jild22-masala-185", "jild22-masala-186", "jild22-masala-187", "jild22-masala-188", "jild22-masala-189", "jild22-masala-190", "jild22-masala-191", "jild22-masala-192", "jild22-masala-193", "jild22-masala-194", "jild22-masala-195", "jild22-masala-196", "jild22-masala-197", "jild22-masala-198", "jild22-masala-199", "jild22-masala-200", "jild22-masala-201", "jild22-masala-202", "jild22-masala-203", "jild22-masala-204", "jild22-masala-205-208", "jild22-masala-209", "jild22-masala-210-212", "jild22-masala-213", "jild22-masala-214", "jild22-masala-215", "jild22-masala-216", "jild22-masala-217-219", "jild22-masala-220", "jild22-masala-221-226", "jild22-masala-227", "jild22-masala-228", "jild22-masala-229", "jild22-masala-230", "jild22-masala-231", "jild22-masala-232-233", "jild22-masala-234", "jild22-masala-235", "jild22-masala-236", "jild22-masala-237", "jild22-masala-238", "jild22-masala-239", "jild22-masala-240", "jild22-masala-241", "jild22-khatima"]

# v3 pages per item id, verbatim from content/tashreeh/jild-22.json.
V3_PAGES = {
    "jild22-pesh-lafz": [5, 6, 7, 8, 9, 10],
    "jild22-masala-1": [111, 112, 113, 114, 115],
    "jild22-masala-2": [115],
    "jild22-masala-3": [115, 116],
    "jild22-masala-4": [116, 117, 118, 119, 120, 121, 122, 123, 124, 125],
    "jild22-masala-5": [126, 127, 128],
    "jild22-masala-6-7": [128, 129],
    "jild22-masala-8-9": [129],
    "jild22-masala-10": [129, 130],
    "jild22-masala-11-12": [130],
    "jild22-masala-13-14": [130],
    "jild22-masala-15-16": [130],
    "jild22-masala-17": [131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145],
    "jild22-masala-18": [145, 146, 147],
    "jild22-masala-19": [147, 148],
    "jild22-masala-20": [148, 149, 150],
    "jild22-masala-21": [150, 151, 152],
    "jild22-masala-22": [152],
    "jild22-masala-23-24": [152, 153],
    "jild22-masala-25": [153, 154],
    "jild22-masala-26": [155, 156, 157],
    "jild22-masala-27": [157, 158, 159, 160, 161, 162, 163, 164],
    "jild22-masala-28": [164, 165, 166, 167, 168, 169],
    "jild22-masala-29-30": [169, 170, 171],
    "jild22-masala-31": [172],
    "jild22-masala-32": [173, 174, 175],
    "jild22-masala-33": [175],
    "jild22-masala-34": [175, 176, 177],
    "jild22-masala-35": [177, 178, 179, 180, 181],
    "jild22-masala-36": [182, 183],
    "jild22-masala-37": [183, 184],
    "jild22-masala-38": [184],
    "jild22-masala-39": [184, 185],
    "jild22-masala-40": [185],
    "jild22-masala-41": [186],
    "jild22-masala-42": [186, 187],
    "jild22-masala-43": [187, 188],
    "jild22-masala-44": [188],
    "jild22-masala-45": [188],
    "jild22-masala-46-47": [188],
    "jild22-masala-48": [188, 189],
    "jild22-masala-49": [189, 190, 191, 192, 193],
    "jild22-masala-50": [193, 194],
    "jild22-masala-51": [194],
    "jild22-masala-52": [194],
    "jild22-masala-53": [194, 195, 196, 197, 198],
    "jild22-masala-54": [198, 199],
    "jild22-masala-55": [199],
    "jild22-masala-56": [199, 200],
    "jild22-masala-57": [201, 202],
    "jild22-masala-58": [202, 203],
    "jild22-masala-59": [203],
    "jild22-masala-60": [204, 205],
    "jild22-masala-61": [205, 206],
    "jild22-masala-62": [206, 207],
    "jild22-masala-63": [207, 208],
    "jild22-masala-64": [208],
    "jild22-masala-65": [208, 209, 210, 211, 212, 213, 214, 215],
    "jild22-masala-66": [216],
    "jild22-masala-67-71": [216, 217, 218, 219, 220],
    "jild22-masala-72-87": [221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233],
    "jild22-masala-88": [233, 234],
    "jild22-masala-89": [234],
    "jild22-masala-90": [234, 235],
    "jild22-masala-91": [235],
    "jild22-masala-92": [235, 236],
    "jild22-masala-93": [236, 237],
    "jild22-masala-94": [237],
    "jild22-masala-95": [237, 238, 239],
    "jild22-masala-96": [239],
    "jild22-masala-97": [239, 240],
    "jild22-masala-98": [240],
    "jild22-masala-99": [240],
    "jild22-masala-100": [240],
    "jild22-masala-101-102": [240, 241, 242],
    "jild22-masala-103": [242, 243],
    "jild22-masala-104": [243],
    "jild22-masala-105-106": [243],
    "jild22-masala-107": [243, 244],
    "jild22-masala-108": [244],
    "jild22-masala-109-110": [244, 245],
    "jild22-masala-111": [245],
    "jild22-masala-112": [245, 246],
    "jild22-masala-113": [246],
    "jild22-masala-114": [246, 247],
    "jild22-masala-115": [247],
    "jild22-masala-116": [247, 248],
    "jild22-masala-117": [248, 249],
    "jild22-masala-118": [249],
    "jild22-masala-119-120": [249, 250],
    "jild22-masala-121": [251, 252, 253, 254, 255, 256, 257, 258],
    "jild22-masala-122": [258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268],
    "jild22-masala-123": [269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314],
    "jild22-masala-124": [314, 315],
    "jild22-masala-125": [315, 316],
    "jild22-masala-126": [316, 317],
    "jild22-masala-127": [317, 318, 319, 320, 321, 322, 323, 324],
    "jild22-masala-128": [324, 325, 326],
    "jild22-masala-129": [327],
    "jild22-masala-130": [327, 328, 329, 330],
    "jild22-masala-131": [330, 331],
    "jild22-masala-132": [331],
    "jild22-masala-133": [332, 333, 334, 335, 336, 337],
    "jild22-masala-134": [337, 338],
    "jild22-masala-135-139": [338, 339],
    "jild22-masala-140": [341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352],
    "jild22-masala-141": [352, 353, 354, 355],
    "jild22-masala-142": [355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366],
    "jild22-masala-143": [366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377],
    "jild22-masala-144": [378],
    "jild22-masala-145": [378],
    "jild22-masala-146": [378, 379],
    "jild22-masala-147-148": [379],
    "jild22-masala-149": [379, 380],
    "jild22-masala-150": [380, 381],
    "jild22-masala-151": [381, 382],
    "jild22-masala-152": [382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399, 400],
    "jild22-masala-153-154": [400, 401],
    "jild22-masala-155": [401],
    "jild22-masala-156": [401, 402, 403, 404, 405, 406, 407],
    "jild22-masala-157": [407],
    "jild22-masala-158-163": [407, 408, 409],
    "jild22-masala-164-168": [409, 410],
    "jild22-masala-169": [410, 411],
    "jild22-masala-170-171": [411, 412, 413, 414],
    "jild22-masala-172": [414, 415],
    "jild22-masala-173": [415],
    "jild22-masala-174": [416],
    "jild22-masala-175": [416],
    "jild22-masala-176": [416, 417, 418],
    "jild22-masala-177": [418, 419],
    "jild22-masala-178": [419],
    "jild22-masala-179": [419],
    "jild22-masala-180-183": [419, 420, 421, 422],
    "jild22-masala-184": [422, 423],
    "jild22-masala-185": [423],
    "jild22-masala-186": [425, 426, 427, 428, 429, 430, 431, 432, 433, 434, 435, 436, 437, 438, 439, 440, 441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 454, 455, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 466, 467, 468, 469, 470, 471, 472, 473, 474, 475, 476, 477, 478, 479, 480, 481, 482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493, 494, 495, 496, 497, 498, 499, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 516, 517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533, 534, 535, 536, 537, 538, 539, 540, 541, 542],
    "jild22-masala-187": [543],
    "jild22-masala-188": [543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 553, 554, 555, 556, 557, 558, 559, 560, 561, 562],
    "jild22-masala-189": [562, 563],
    "jild22-masala-190": [563],
    "jild22-masala-191": [563],
    "jild22-masala-192": [563, 564],
    "jild22-masala-193": [564],
    "jild22-masala-194": [564, 565],
    "jild22-masala-195": [565],
    "jild22-masala-196": [565, 566, 567],
    "jild22-masala-197": [567],
    "jild22-masala-198": [568],
    "jild22-masala-199": [568],
    "jild22-masala-200": [568],
    "jild22-masala-201": [569],
    "jild22-masala-202": [571, 572, 573, 574],
    "jild22-masala-203": [574, 575, 576],
    "jild22-masala-204": [576, 577, 578, 579, 580],
    "jild22-masala-205-208": [580, 581, 582, 583, 584, 585, 586, 587, 588, 589, 590, 591, 592],
    "jild22-masala-209": [593, 594, 595],
    "jild22-masala-210-212": [595, 596, 597, 598, 599, 600],
    "jild22-masala-213": [600, 601],
    "jild22-masala-214": [601, 602],
    "jild22-masala-215": [602, 603],
    "jild22-masala-216": [603, 604],
    "jild22-masala-217-219": [604, 605],
    "jild22-masala-220": [605],
    "jild22-masala-221-226": [605, 606],
    "jild22-masala-227": [607, 608, 609, 610, 611, 612, 613, 614, 615, 616, 617, 618, 619, 620, 621, 622, 623, 624, 625, 626, 627, 628, 629, 630, 631, 632, 633, 634, 635, 636, 637, 638, 639, 640, 641, 642, 643, 644, 645, 646, 647, 648, 649, 650, 651, 652, 653, 654, 655, 656, 657, 658, 659, 660, 661, 662, 663, 664, 665, 666, 667, 668, 669, 670, 671, 672, 673, 674, 675, 676, 677],
    "jild22-masala-228": [678],
    "jild22-masala-229": [678, 679],
    "jild22-masala-230": [679],
    "jild22-masala-231": [679, 680],
    "jild22-masala-232-233": [680, 681, 682, 683, 684],
    "jild22-masala-234": [685, 686],
    "jild22-masala-235": [686, 687],
    "jild22-masala-236": [687, 688],
    "jild22-masala-237": [688],
    "jild22-masala-238": [689],
    "jild22-masala-239": [689, 690, 691],
    "jild22-masala-240": [691],
    "jild22-masala-241": [691, 692],
    "jild22-khatima": [692],
}

# The Urdu word for "page" is safha; the canonical marker is [safha N].
# Lenticular brackets are U+3010 (open) and U+3011 (close); the short
# form is "[safha-initial N]" with the single-letter abbreviation.
# All three patterns are built from escapes so this file never contains
# a literal lenticular-bracket pair.
SAFHA = "\u0635\u0641\u062d\u06c1"  # the Urdu word for page
RE_LENTICULAR = re.compile("\u3010" + SAFHA + " (\\d+)\u3011")
RE_SHORT = re.compile("\\[\u0635 (\\d+)\\]")
RE_FULL = re.compile("\\[" + SAFHA + " \\d+\\]")
LENTICULAR_PREFIX = "\u3010" + SAFHA  # for the "none remain" check
SHORT_PREFIX_RE = re.compile("\\[\u0635 \\d+\\]")

# Canonical output key order for every item (the JSON contract).
ITEM_KEYS = ["id", "title", "volume_id", "pages", "start_page",
             "tashreeh_sawal", "tashreeh_jawab", "notes"]


def normalise_markers(text):
    """Normalise page markers in one string. Returns (new_text, counts)."""
    counts = {"lenticular": 0, "short": 0, "full_before": 0}
    counts["full_before"] = len(RE_FULL.findall(text))
    text, n1 = RE_LENTICULAR.subn("[" + SAFHA + " \\1]", text)
    counts["lenticular"] = n1
    text, n2 = RE_SHORT.subn("[" + SAFHA + " \\1]", text)
    counts["short"] = n2
    return text, counts


def normalise_item(item):
    """Normalise markers in every string field of one item (recursively
    through lists/dicts). Returns per-form counts for this item."""
    totals = {"lenticular": 0, "short": 0, "full_before": 0}

    def walk(value):
        if isinstance(value, str):
            new, c = normalise_markers(value)
            for k in totals:
                totals[k] += c[k]
            return new
        if isinstance(value, list):
            return [walk(v) for v in value]
        if isinstance(value, dict):
            return {k: walk(v) for k, v in value.items()}
        return value

    for key in list(item.keys()):
        item[key] = walk(item[key])
    return totals


def main():
    assert len(V3_IDS) == EXPECTED_ITEMS, "embedded V3_IDS length mismatch"
    assert set(V3_PAGES) == set(V3_IDS), "embedded V3_PAGES keys != V3_IDS"
    assert sum(len(v) for v in V3_PAGES.values()) >= EXPECTED_PAGES

    manifest = json.load(open(os.path.join(BASE, MANIFEST), encoding="utf-8"))
    entries = manifest["chunks"]
    assert len(entries) == 12, f"expected 12 manifest entries, got {len(entries)}"
    assert all(e.get("status") == "done" for e in entries), "not all chunks done"
    by_file = {e["file"]: e for e in entries}
    assert set(by_file) == set(CHUNK_ORDER), (
        f"manifest files != CHUNK_ORDER: "
        f"missing={set(CHUNK_ORDER) - set(by_file)} "
        f"extra={set(by_file) - set(CHUNK_ORDER)}"
    )

    all_items = []
    derived = []
    for fname in CHUNK_ORDER:
        path = os.path.join(BASE, fname)
        d = json.load(open(path, encoding="utf-8"))
        items = d["masail"]
        entry = by_file[fname]
        assert len(items) == entry["items"], (
            f"{fname}: {len(items)} items, manifest says {entry['items']}"
        )
        # Marker normalisation, with before/after counts per chunk.
        before = {"lenticular": 0, "short": 0, "full_before": 0}
        for it in items:
            c = normalise_item(it)
            for k in before:
                before[k] += c[k]
        after_full = sum(
            len(RE_FULL.findall(json.dumps(it, ensure_ascii=False)))
            for it in items
        )
        print(
            f"{fname}: {len(items)} items | markers before: "
            f"lenticular={before['lenticular']} short={before['short']} "
            f"full={before['full_before']} | after: full={after_full}"
        )
        assert after_full == (
            before["lenticular"] + before["short"] + before["full_before"]
        ), f"{fname}: marker count changed by normalisation!"
        # Derive start_page = min(pages) where absent (none expected: all chunks carry start_page).
        for it in items:
            if "start_page" not in it:
                it["start_page"] = min(it["pages"])
                derived.append(it["id"])
        all_items.extend(items)

        # SIMPLE JOINS (4): parts found BY ID anywhere. Sawal mode:
    # "first" (the single non-empty sawal, in the first part).
    # CROSS-ID JOINS (2): 123 (head + safaih-allujayn tail), 186 (3-part-joined head + hawashi-zubda tail).
    # Sawal = head.

    def join_consecutive(item_id, expected_parts, seam_checks, sawal_mode="first", title_suffix=None):
        idxs = [i for i, it in enumerate(all_items) if it["id"] == item_id]
        assert len(idxs) == expected_parts, (
            f"{item_id}: expected {expected_parts} parts, found {len(idxs)}"
        )
        parts = [all_items[i] for i in idxs]
        assert [min(p["pages"]) for p in parts] == sorted(min(p["pages"]) for p in parts), (
            f"{item_id}: parts not in page order"
        )
        vols = {p.get("volume_id") for p in parts}
        assert vols == EXPECTED_VOLUME_IDS, f"{item_id}: volume mismatch {vols}"
        for part_no, which, needle in seam_checks:
            txt = parts[part_no].get("tashreeh_jawab") or ""
            if which == "end":
                assert txt.rstrip().endswith(needle), (
                    f"{item_id} part {part_no + 1} end seam mismatch: ...{txt.rstrip()[-60:]!r}"
                )
            else:
                assert txt.lstrip().startswith(needle), (
                    f"{item_id} part {part_no + 1} start seam mismatch: {txt.lstrip()[:60]!r}"
                )
        if sawal_mode == "first":
            sawals = [p.get("tashreeh_sawal") for p in parts if p.get("tashreeh_sawal")]
            assert len(sawals) <= 1, f"{item_id}: {len(sawals)} non-empty sawals"
            if sawals:
                assert parts[0].get("tashreeh_sawal"), f"{item_id}: non-empty sawal not in first part"
            sawal = sawals[0] if sawals else ""
        else:
            raise AssertionError(f"{item_id}: unknown sawal_mode {sawal_mode}")
        title = parts[0]["title"]
        if title_suffix:
            assert title.endswith(title_suffix), f"{item_id}: first-part title lacks suffix {title_suffix!r}"
            assert all(p["title"].endswith(title_suffix) for p in parts), f"{item_id}: a part title lacks the suffix"
            title = title[: -len(title_suffix)]
        assert title == V3_TITLES[item_id], f"{item_id}: joined title != v3 title"
        pages = sorted({p for part in parts for p in part["pages"]})
        # A head that is also a CROSS-JOIN head (masala-186) gains its
        # cross tails' pages only at the cross join: expect v3 pages
        # minus them here.
        cross_tails = {"jild22-masala-186": ["jild22-hawashi-zubda"]}
        cross_tail_pages = {p for t in cross_tails.get(item_id, []) for it in all_items if it["id"] == t for p in it["pages"]}
        assert pages == sorted(set(V3_PAGES[item_id]) - cross_tail_pages), f"{item_id}: joined pages != v3"
        joined = {
            "id": item_id,
            "title": title,
            "volume_id": parts[0]["volume_id"],
            "pages": pages,
            "start_page": min(pages),
            "tashreeh_sawal": sawal,
            "tashreeh_jawab": " ".join(p["tashreeh_jawab"].strip() for p in parts if p.get("tashreeh_jawab")),
            "notes": [n for p in parts for n in p.get("notes", [])],
        }
        first = idxs[0]
        for i in reversed(idxs[1:]):
            del all_items[i]
        all_items[first] = joined
        print(f"joined {item_id}: {expected_parts} parts -> 1 item, {len(pages)} pages")

    def join_cross_id(result_id, head_id, tail_ids, sawal_mode, seam_checks):
        seq = [head_id] + tail_ids
        idxs = [i for i, it in enumerate(all_items) if it["id"] in seq]
        assert [all_items[i]["id"] for i in idxs] == seq, (
            f"{result_id}: cross-id order mismatch"
        )
        parts = [all_items[i] for i in idxs]
        vols = {p.get("volume_id") for p in parts}
        assert vols == EXPECTED_VOLUME_IDS, f"{result_id}: volume mismatch {vols}"
        if sawal_mode == "head_only":
            assert parts[0].get("tashreeh_sawal"), f"{result_id}: head sawal empty"
            assert all(not p.get("tashreeh_sawal") for p in parts[1:]), (
                f"{result_id}: a tail sawal is non-empty"
            )
            sawal = parts[0]["tashreeh_sawal"]
        else:
            raise AssertionError(f"{result_id}: unknown sawal_mode {sawal_mode}")
        for part_no, which, needle in seam_checks:
            txt = parts[part_no].get("tashreeh_jawab") or ""
            if which == "end":
                assert txt.rstrip().endswith(needle), (
                    f"{result_id} part {part_no + 1} end seam mismatch: ...{txt.rstrip()[-60:]!r}"
                )
            else:
                assert txt.lstrip().startswith(needle), (
                    f"{result_id} part {part_no + 1} start seam mismatch: {txt.lstrip()[:60]!r}"
                )
        pages = sorted({p for part in parts for p in part["pages"]})
        assert pages == V3_PAGES[result_id], f"{result_id}: joined pages != v3"
        title = parts[0]["title"]
        assert title == V3_TITLES[result_id], f"{result_id}: joined title != v3 title"
        joined = {
            "id": result_id,
            "title": title,
            "volume_id": parts[0]["volume_id"],
            "pages": pages,
            "start_page": min(pages),
            "tashreeh_sawal": sawal,
            "tashreeh_jawab": " ".join(p["tashreeh_jawab"].strip() for p in parts if p.get("tashreeh_jawab")),
            "notes": [n for p in parts for n in p.get("notes", [])],
        }
        first = idxs[0]
        for i in reversed(idxs[1:]):
            del all_items[i]
        all_items[first] = joined
        print(f"joined {result_id}: {1 + len(tail_ids)} parts -> 1 item, {len(pages)} pages")

    V3_TITLES = {
        "jild22-masala-152": "مسئلہ ۱۵۲",
        "jild22-masala-186": "مسئلہ ۱۸۶",
        "jild22-masala-205-208": "مسئلہ ۲۰۵ تا ۲۰۸: داڑھی ایک مشت سے زیادہ رکھنا؛ حضور و حضرت علی کی ریش مبارک (پہلا حصہ)",
        "jild22-masala-227": "مسئلہ ۲۲۷",
        "jild22-masala-123": "مسئلہ ۱۲۳: دونوں ہاتھ سے مصافحہ جائز ہے یا نہیں؟ (رسالہ صفائح اللجین فی کون التصافح بکفی الیدین ۱۳۰۶ھ)",
    }

    join_consecutive(
        "jild22-masala-152", 2,
        [
            (0, "end", "تے ہیں — یہیں صفحہ 390 پر جواب جاری ہے اور اس کے بعد کی بحث اگلے حصے میں آئے گی۔"),
            (1, "start", "یہ مسئلہ ۱۵۲ کے جواب کا بقیہ حصہ ہے جو صفحہ ۳۹۱ سے جاری ہے؛ سوال اور جواب کا آغا"),
        ],
        sawal_mode="first",
        title_suffix=None,
    )
    join_consecutive(
        "jild22-masala-186", 3,
        [
            (0, "end", " سجدہ کرے۔ یہاں صفحہ ۴۴۰ ختم ہوتا ہے اور حدیث چہارم کا بقیہ اگلے حصے میں آئے گا۔"),
            (1, "start", "جواب کے اس حصے سے پہلے چوتھی حدیث کا آخری ٹکڑا آتا ہے کہ اگر میں کسی کو سجدے کا "),
            (1, "end", "ستے کا فرق کہ کہاں سے کہاں تک ہے، اور یہ بحث صفحہ چار سو نوے کے بعد بھی جاری ہے۔"),
            (2, "start", "یہ حصہ رسالے کے اسی سلسلے کا تسلسل ہے جس میں اعلیٰ حضرت رحمۃ اللہ علیہ مخالف کی "),
        ],
        sawal_mode="first",
        title_suffix=None,
    )
    join_consecutive(
        "jild22-masala-205-208", 2,
        [
            (0, "end", "رتی طور پر ہی مٹھی بھر تھی اور گھٹتی بڑھتی نہ تھی، پس اس کا حوالہ نہیں پایا گیا۔"),
            (1, "start", "یہ مسئلہ ۲۰۵ تا ۲۰۸ کے جواب کا آخری حصہ ہے، جس میں چوتھے سوال یعنی حضور اقدس صلی"),
        ],
        sawal_mode="first",
        title_suffix=None,
    )
    join_consecutive(
        "jild22-masala-227", 2,
        [
            (0, "end", " کیا کہ یہ اشراط الساعۃ میں سے ہے، اور اشراط الساعۃ کا مطلب ہے قیامت کی نشانیاں۔"),
            (1, "start", "صفحہ ۶۴۱ سے رسالے کا بقیہ حصہ شروع ہوتا ہے۔ پہلے وجہ ثامن یعنی آٹھویں دلیل میں ق"),
        ],
        sawal_mode="first",
        title_suffix=None,
    )
    join_cross_id(
        "jild22-masala-123",
        "jild22-masala-123",
        ["jild22-safaih-allujayn"],
        "head_only",
        [
            (0, "end", "یہ مسئلہ صفحہ ۳۰۰ پر ختم نہیں ہوا؛ باقی حصہ اگلے چنک میں آئے گا۔]"),
            (1, "start", "یہ حصہ رسالے کے اسی سلسلے کی کڑی ہے جو پہلے سے چل رہا ہے۔ اعلیٰ حضرت پہلے ایک عج"),
        ],
    )
    join_cross_id(
        "jild22-masala-186",
        "jild22-masala-186",
        ["jild22-hawashi-zubda"],
        "head_only",
        [
            (0, "end", "ی صفحے ۵۴۰ پر تیسرے تنبیہ کے درمیان میں جاری ہے، اس کا بقیہ اگلے حصے میں آئے گا۔"),
            (1, "start", "یہ حصہ اس رسالے کے آخر میں شامل عربی حاشیوں کا بقیہ ہے جو رسالے کے مختلف صفحات ا"),
        ],
    )
    assert len(all_items) == len({it["id"] for it in all_items}), (
        f"DUPLICATE IDS after joins: "
        f"{[i for i in [it['id'] for it in all_items] if [it['id'] for it in all_items].count(i) > 1]}"
    )

    print(f"after joins: {len(all_items)} items (187 concatenated)")
    # NO renames at merge: the two tail ids (safaih-allujayn,
    # hawashi-zubda) are absorbed by their cross-id joins.
    print("renames applied: [] (tails absorbed by cross-id joins)")
    # STABLE SORT by start_page, mirroring the v3 merge.
    all_items.sort(key=lambda it: it["start_page"])
    print(f"sorted by start_page: {len(all_items)} items")
    print(f"start_page derived for {len(derived)} items (expected 0: all chunks carry start_page)")
    assert len(all_items) == EXPECTED_ITEMS, (
        f"expected {EXPECTED_ITEMS} items, got {len(all_items)}"
    )

    # Rebuild each item in the canonical key order (the JSON contract).
    canonical = []
    for it in all_items:
        assert set(it.keys()) == set(ITEM_KEYS), (
            f"{it.get('id')}: keys {sorted(it.keys())} != {sorted(ITEM_KEYS)}"
        )
        canonical.append({k: it[k] for k in ITEM_KEYS})
    all_items = canonical

    # Validation.
    ids = [it["id"] for it in all_items]
    assert ids == V3_IDS, (
        f"ID sequence != v3: first diff at "
        f"{next((i for i, (a, b) in enumerate(zip(ids, V3_IDS)) if a != b), min(len(ids), len(V3_IDS)))}"
    )
    assert len(ids) == len(set(ids)), (
        f"DUPLICATE IDS: {[i for i in ids if ids.count(i) > 1]}"
    )
    empty = [it["id"] for it in all_items if not it.get("tashreeh_jawab")]
    assert not empty, f"EMPTY JAWAB: {empty}"
    vols = set(it.get("volume_id") for it in all_items)
    assert vols == EXPECTED_VOLUME_IDS, f"volume_ids {vols} != {EXPECTED_VOLUME_IDS}"
    pages = set()
    for it in all_items:
        assert it["pages"] == V3_PAGES[it["id"]], (
            f"{it['id']}: pages {it['pages']} != v3 {V3_PAGES[it['id']]}"
        )
        pages.update(it["pages"])
    assert len(pages) == EXPECTED_PAGES, (
        f"expected {EXPECTED_PAGES} unique pages, got {len(pages)}"
    )
    assert min(pages) == EXPECTED_MIN_PAGE, f"min page {min(pages)}"
    assert max(pages) == EXPECTED_MAX_PAGE, f"max page {max(pages)}"
    exceptions = {
        it["id"] for it in all_items if it["start_page"] != min(it["pages"])
    }
    assert not exceptions, f"start_page != min(pages): {sorted(exceptions)}"
    assert ids[0] == EXPECTED_FIRST_ID, f"first id {ids[0]}"
    assert ids[-1] == EXPECTED_LAST_ID, f"last id {ids[-1]}"

    out = {"masail": all_items}
    out_text = json.dumps(out, ensure_ascii=False, indent=1)
    assert LENTICULAR_PREFIX not in out_text, "lenticular markers remain in output!"
    assert not SHORT_PREFIX_RE.search(out_text), "short markers remain in output!"
    total_markers = len(RE_FULL.findall(out_text))
    print(
        f"VALIDATION PASSED: {len(all_items)} items, {len(pages)} unique pages "
        f"(min {min(pages)}, max {max(pages)}), volume_ids {sorted(vols)}, "
        f"first {ids[0]}, last {ids[-1]}, start_page == min(pages) for all, "
        f"derived start_page for {len(derived)} items, markers {total_markers}"
    )

    with open(os.path.join(BASE, "jild-22.json"), "w", encoding="utf-8") as f:
        f.write(out_text)
    print(f"WROTE jild-22.json: {len(out_text.encode('utf-8'))} bytes")


if __name__ == "__main__":
    main()
