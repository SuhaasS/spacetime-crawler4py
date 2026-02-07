# Assignment 1 Part A
# Suhaas Surapaneni

import sys

def tokenize_generator(file_path):
    """
    Runtime Complexity: O(n)
    where n is the total number of characters in the input file.
    The function scans each character exactly once and performs
    constant-time operations per character.
    """
    current_token = ""

    # File-level exceptions will propagate
    with open(file_path, "r", encoding="utf-8", errors="ignore") as file:
        for line in file:
            for char in line:
                try:
                    if char.isascii() and char.isalnum():
                        current_token += char.lower()
                    else:
                        if current_token:
                            yield current_token
                            current_token = ""
                except Exception:
                    continue

    if current_token:
        yield current_token


def compute_word_frequencies(tokens):
    """
    Runtime Complexity: O(T) where T is the total number of tokens.
    Each token is processed once, and dictionary operations are O(1).
    """
    frequencies = {}
    for token in tokens:
        if token in frequencies:
            frequencies[token] += 1
        else:
            frequencies[token] = 1
    return frequencies


def get_frequency(item):
    """
    Runtime Complexity: O(1)
    Simply returns the frequency value from a tuple.
    """
    return item[1]


def print_frequencies(frequencies):
    """
    Runtime Complexity: O(nlogn)
    Where n is the number of unique tokens.
    Sorting takes O(nlogn) time while printing is O(n), which 
    means O(nlogn) + O(n) yields O(nlogn)
    """
    items = list(frequencies.items())
    items.sort(key=get_frequency, reverse=True)
    for token, count in items:
        print(f"{token}\t{count}")


def main():
    """
    Runtime Complexity: O(N + nlogn)
    Tokenization runs in linear time relative to file size.
    Frequency computation is linear in number of tokens.
    Sorting unique tokens dominates with O(nlogn).
    main calls the functions above, yielding O(N + nlogn)
    """
    if len(sys.argv) != 2:
        raise ValueError("Usage: python PartA.py <text_file>")

    file_path = sys.argv[1]

    tokens = list(tokenize_generator(file_path))
    frequencies = compute_word_frequencies(tokens)
    print_frequencies(frequencies)


if __name__ == "__main__":
    main()