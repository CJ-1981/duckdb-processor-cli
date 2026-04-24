import json

# We want to find s, t such that:
# 1. set(s) == set(t)
# 2. s and t are lists of string that are already sorted!
# 3. json.dumps(s) != json.dumps(t)

# Scenario A: Duplicates (already discussed)
s = sorted(["A", "A"])
t = sorted(["A"])
print("Scenario A - Duplicates:")
print(f"s: {s}")
print(f"t: {t}")
print(f"set(s) == set(t): {set(s) == set(t)}")
print(f"json diff: {json.dumps(s) != json.dumps(t)}")

# Are there any other scenarios where two lists of strings,
# both sorted(set(...)) evaluate to identical, but JSON diff does not?
# Wait! In the OLD code, it was NOT set()!
# The old code:
# result = sorted(str(v).strip() for v in parsed if str(v).strip())
# So result is a sorted list.
# Is it possible that the OLD code produced lists where set(s) == set(t) AND json.dumps(s) != json.dumps(t)?

# If s and t are two lists of strings.
# s is sorted.
# t is sorted.
# set(s) == set(t)
# json.dumps(s) != json.dumps(t)
# THIS IS ONLY POSSIBLE IF their elements have different frequencies! (i.e. duplicates!)
