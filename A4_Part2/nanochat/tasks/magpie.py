import os
import json
from datasets import load_dataset

def prepare_magpie():
    print("Downloading Magpie-Pro-300K-Filtered...")
    # Using the filtered H4 version which is high quality
    ds = load_dataset("HuggingFaceTB/Magpie-Pro-300K-Filtered-H4", split="train_sft")
    
    output_path = "data/magpie_pro_300k.jsonl"
    os.makedirs("data", exist_ok=True)
    
    print(f"Converting to {output_path}...")
    with open(output_path, "w", encoding="utf-8") as f:
        for entry in ds:
            # Magpie usually comes in a 'conversations' list format
            # We map it to the standard format your CustomJSON likely expects: 
            # {"conversations": [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}]}
            
            # The H4 version already has 'conversations' key in the right format
            # but we ensure it's a valid JSON line
            json_line = json.dumps({"conversations": entry["conversations"]})
            f.write(json_line + "\n")
            
    print("Done! You can now use 'data/magpie_pro_300k.jsonl' in your SFT script.")

if __name__ == "__main__":
    prepare_magpie()