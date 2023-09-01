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

def add_custom_image(file, slide, placeholder):
    """
    Utility method for adding a saved image to a slide in place of an existing shape.

    params:
    file - either a string path to an image file, or a file-like object containing bytes.
    slide - the pptx slide object.
    placeholder - an existing shape to copy the dimensions of.
    """
    if type(file) == str:
        file = open(file, "rb")
    elif type(file) == bytes:
        pass

    slide.shapes.add_picture(
        image_file = file,
        left=placeholder.left,
        top=placeholder.top,
        width=placeholder.width,
        height=placeholder.height
    )