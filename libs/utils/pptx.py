def replace_text(replacements, shapes):
    """
    Utility function to replace text in a list of pptx shapes.
     
    params:
    replacements - a dictionary of text replacements to execute.
    shapes - the list of shapes to iterate through and check for replacement keywords.
    """
    # NOTE:
    # There are some edge cases here where PPT will split up text into multiple runs when it seems like it shouldn't.
    # Notably, if your placeholder names are flagged as misspelled words, the runs will be split into ['{{', 'badtext', '}}'].
    # This will cause the placeholder to not be recognized.
    # Some possible fixes here {https://stackoverflow.com/a/56226142}, or just stick to placeholder names that exist in the PPT dictionary.
    for shape in shapes:
        if shape.has_text_frame:
            text_frame = shape.text_frame
            for paragraph in text_frame.paragraphs:
                for run in paragraph.runs:
                    for key, value in replacements.items():
                      run.text = run.text.replace(str(key), str(value))

from io import BytesIO

def _normalize_image(file):
    if isinstance(file, str):
        with open(file, "rb") as f:
            return BytesIO(f.read())

    if isinstance(file, (bytes, bytearray)):
        return BytesIO(bytes(file))

    # file-like
    try:
        file.seek(0)
    except Exception:
        pass

    data = file.read()
    if not isinstance(data, (bytes, bytearray)):
        raise TypeError(f"Unsupported image type: {type(data)}")

    return BytesIO(bytes(data))


def add_custom_image(file, slide, placeholder):
    stream = _normalize_image(file)

    slide.shapes.add_picture(
        image_file=stream,
        left=placeholder.left,
        top=placeholder.top,
        width=placeholder.width,
        height=placeholder.height,
    )
