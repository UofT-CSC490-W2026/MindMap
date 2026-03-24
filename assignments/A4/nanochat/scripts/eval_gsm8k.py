import argparse
import os
import re
import torch

from datasets import load_dataset
from contextlib import nullcontext

from nanochat.common import compute_init, autodetect_device_type
from nanochat.checkpoint_manager import load_model
from nanochat.engine import Engine


def extract_final_answer(text: str) -> str:
    text = text.strip()

    m = re.search(r"####\s*([-+]?\d[\d,]*\.?\d*)", text)
    if m:
        return m.group(1).replace(",", "")

    matches = re.findall(r"[-+]?\d[\d,]*\.?\d*", text)
    if matches:
        return matches[-1].replace(",", "")

    return "N/A"


def main():
    parser = argparse.ArgumentParser(description="Evaluate Nanochat model on GSM8K")
    parser.add_argument("-i", "--source", type=str, default="sft", help="sft|rl")
    parser.add_argument("-g", "--model-tag", type=str, required=True, help="Model tag to load")
    parser.add_argument("-s", "--step", type=int, required=True, help="Checkpoint step to load")
    parser.add_argument("-n", "--num-examples", type=int, default=30, help="Number of GSM8K test examples")
    parser.add_argument("--max-tokens", type=int, default=128, help="Max generation tokens")
    parser.add_argument("-t", "--temperature", type=float, default=0.0, help="Generation temperature")
    parser.add_argument("-k", "--top-k", type=int, default=50, help="Top-k sampling")
    parser.add_argument("--device-type", type=str, default="", choices=["cuda", "cpu", "mps"], help="empty => autodetect")
    parser.add_argument("-d", "--dtype", type=str, default="bfloat16", choices=["float32", "bfloat16"])
    args = parser.parse_args()

    dataset = load_dataset("gsm8k", "main")["test"]

    device_type = autodetect_device_type() if args.device_type == "" else args.device_type
    ddp, ddp_rank, ddp_local_rank, ddp_world_size, device = compute_init(device_type)

    ptdtype = torch.float32 if args.dtype == "float32" else torch.bfloat16
    autocast_ctx = (
        torch.amp.autocast(device_type=device_type, dtype=ptdtype)
        if device_type == "cuda"
        else nullcontext()
    )

    print(
        f"Loading model: source={args.source}, "
        f"model_tag={args.model_tag}, step={args.step}, device={device}"
    )

    model, tokenizer, meta = load_model(
        args.source,
        device,
        phase="eval",
        model_tag=args.model_tag,
        step=args.step,
    )

    engine = Engine(model, tokenizer)

    out_dir = "/data/.cache/nanochat"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(
        out_dir,
        f"gsm8k_eval_{args.source}_{args.model_tag}_{args.step}.txt",
    )

    correct_count = 0

    with open(out_path, "w", encoding="utf-8") as f:
        for i in range(min(args.num_examples, len(dataset))):
            question = dataset[i]["question"]
            gt_full = dataset[i]["answer"]
            gt_final = extract_final_answer(gt_full)

            prompt = f"Question: {question}\nAnswer:"
            bos = tokenizer.get_bos_token_id()
            prompt_tokens = [bos] + tokenizer.encode(prompt)

            generate_kwargs = {
                "num_samples": 1,
                "max_tokens": args.max_tokens,
                "temperature": args.temperature,
                "top_k": args.top_k,
            }

            response_tokens = []
            with autocast_ctx:
                for token_column, token_masks in engine.generate(prompt_tokens, **generate_kwargs):
                    token = token_column[0]
                    response_tokens.append(token)

            raw_output = tokenizer.decode(response_tokens).strip()
            pred_final = extract_final_answer(raw_output)

            correct = pred_final == gt_final
            if correct:
                correct_count += 1

            block = []
            block.append("=" * 100)
            block.append(f"Example {i + 1}")
            block.append("")
            block.append("QUESTION:")
            block.append(question)
            block.append("")
            block.append("MODEL OUTPUT:")
            block.append(raw_output)
            block.append("")
            block.append(f"PREDICTED FINAL ANSWER: {pred_final}")
            block.append(f"GROUND TRUTH FINAL ANSWER: {gt_final}")
            block.append(f"CORRECT: {correct}")
            block.append("")

            text_block = "\n".join(block)
            print(text_block, flush=True)
            f.write(text_block + "\n")

        accuracy = correct_count / min(args.num_examples, len(dataset))
        summary = (
            "\n" + "=" * 100 + "\n"
            f"FINAL ACCURACY: {correct_count}/{min(args.num_examples, len(dataset))} = {accuracy:.4f}\n"
            f"RESULTS SAVED TO: {out_path}\n"
        )
        print(summary, flush=True)
        f.write(summary)

    print(f"Done. Results saved to {out_path}")


if __name__ == "__main__":
    main()