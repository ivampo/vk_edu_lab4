from collections import Counter
from itertools import combinations


def is_python(row):
    return row["role"] == "Программист, разработчик" and "python" in row["name"].lower()


def count_pairs(rows):
    counts = Counter()
    selected = without_skills = 0
    for row in rows:
        if not is_python(row):
            continue
        skills = set()
        for skill in row["key_skills"].split(";"):
            skill = skill.strip(" \t\r\n").lower()
            if skill:
                skills.add(skill)
        selected += 1
        without_skills += not skills
        counts.update(combinations(sorted(skills), 2))
    return counts, selected, without_skills
