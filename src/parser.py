from typing import List


def contains_keywords(text: str, keywords: List[str]) -> bool:
    if not keywords:
        return True

    lowered = text.lower()
    return any(keyword in lowered for keyword in keywords)


def process_message(text: str) -> str:
    cleaned = " ".join(text.split())
    return cleaned
