def extract_answers(json_data):
    # Initialize an empty dictionary for the answers
    answer_dict = {}
    
    # Create a lookup dictionary for field IDs and their labels
    field_dict = {str(field["id"]): field["label"] for field in json_data["fields"]}
    
    # Create a lookup dictionary for input IDs and their labels within each field
    input_dict = {str(input["id"]): input["label"] for field in json_data["fields"] for input in (field.get("inputs") or [])}
    
    # Loop through each answer
    for id, answer in json_data["answers"].items():
        # Check if the answer is another dict (like for the Name field)
        if isinstance(answer, dict):
            for sub_id, sub_answer in answer.items():
                # Only include the sub-answer if it is not empty
                if sub_answer:
                    # Create the key using the main field label and the subfield label
                    key = f"{field_dict[id]} {input_dict[sub_id]}"
                    answer_dict[key] = sub_answer
        else:
            # Add the answer to the dictionary with the appropriate label
            answer_dict[field_dict[id]] = answer
            
    return answer_dict