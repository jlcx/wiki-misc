# label conversion for months in Hong Kong, created by Gemini 2.5 Pro (preview)

import re

def translate_hk_month(label):
    """
    Translates a Chinese label for a specific month in Hong Kong to English.

    This function takes a string in Chinese, which includes the year, month,
    and the word '香港' (Hong Kong), and converts it into a standardized
    English format. The function can handle cases where '香港' appears at
    the beginning or at the end of the label.

    Args:
        label (str): The Chinese label, e.g., '香港2013年12月' or '2007年1月香港'.

    Returns:
        str: The translated label in English, e.g., 'December 2013 in Hong Kong'.
             Returns an error message if the format is not recognized.
    """
    # Regular expression to find the year and month in the label.
    # \d{4} matches a four-digit year.
    # \d{1,2} matches a one or two-digit month.
    match = re.search(r'(\d{4})年(\d{1,2})月', label)

    if match:
        year = match.group(1)
        month_chinese = int(match.group(2))

        # Mapping of Chinese month numbers to English month names.
        month_map = {
            1: 'January', 2: 'February', 3: 'March', 4: 'April',
            5: 'May', 6: 'June', 7: 'July', 8: 'August',
            9: 'September', 10: 'October', 11: 'November', 12: 'December'
        }

        # Get the English month name from the map.
        month_english = month_map.get(month_chinese)

        if month_english:
            # Format the final English string.
            return f'{month_english} {year} in Hong Kong'

    # Return an error message if the label does not match the expected format.
    return 'Invalid format'

# Example Usage:
label1 = '香港2013年12月'
label2 = '2007年1月香港'
label3 = '2023年5月' # Example of a label without '香港'

translated_label1 = translate_hk_month(label1)
translated_label2 = translate_hk_month(label2)
translated_label3 = translate_hk_month(label3) # This will also be handled correctly.


print(f"'{label1}' -> '{translated_label1}'")
print(f"'{label2}' -> '{translated_label2}'")
print(f"'{label3}' -> '{translated_label3}'")
