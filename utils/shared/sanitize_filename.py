
def sanitize_filename(input: str, 
                      disallowed: list[str] = ['<', '>', ':', '"', '/', '\\', '|', '?', '*'], 
                      allow_spaces: bool = True
                      ) -> str:
    """
    Sanitize a string to be used as (part of) a filename.

    Args:
        input (str): The input string to sanitize.
        disallowed_chars (list[str]): A list of characters to replace with a period. Defaults to ['<', '>', ':', '"', '/', '\\', '|', '?', '*'].
        allow_spaces (bool, optional): Whether to allow spaces in the sanitized string. Defaults to True.

    Returns:
        str: The sanitized string, suitable for use as a filename component.
    """
    if not allow_spaces:
        disallowed.append(' ')

    for char in disallowed:
        input = input.replace(char, ".")
    input = '.'.join(filter(None, input.split('.')))
    return input
