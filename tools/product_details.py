import base64
import json
import mimetypes
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, List, Tuple

import httpx
import pandas as pd
import tiktoken
import tqdm
from dotenv import load_dotenv
from gradio_client import Client, handle_file
from openai import OpenAI

load_dotenv()

SYSTEM_PROMPT = (
    "You are an AI assistant that processes product images and extracts visual and textual details "
    "from them. You will extract product names, classify packaging, detect brand names, "
    "classify materials, extract quantity and units, identify flavors and classify the product in sub-categories. "
    "You must always provide your response as JSON, in the specified format."
)

USER_PROMPT = (
    "On the image that I provide, you must perform the following 7 tasks:\n"
    "1.Classify the packaging of the product in one of the following 13 categories:\n"
    " - bottle\n"
    " - can\n"
    " - bag\n"
    " - carton\n"
    " - box\n"
    " - bucket\n"
    " - drum\n"
    " - jar\n"
    " - pack\n"
    " - stick\n"
    " - tablet\n"
    " - tube\n"
    " - other\n"
    "2.Detect the brand of the product:\n"
    " - you must detect the brand that manufactures the specific product\n"
    "3.Detect the product name:\n"
    " - you must detect the name of the product in the image\n"
    "4.Classify the material used in the packaging, in one of the 5 following categories:\n"
    " - glass\n"
    " - plastic\n"
    " - metal\n"
    " - cardboard\n"
    " - other\n"
    "5.Detect the quantity and the units on the package, if available:\n"
    " - the output should be a string formatted like this: '<quantity>_<units>'\n"
    " - if any of the quantity or units are not written on the package, output 'none' for that missing value\n"
    " - the possible units are:"
    " 'g' for grams, 'kg' for kilograms, 'mL' for mililiters, 'L' for liters, or 'pcs' for pieces\n"
    " - for example: for quantity 20, but no units, output '20_none'; if there is no quantity,"
    " but the mililiter unit is present, output should be 'none_ml'\n"
    "6.Detect the flavour of the product:\n"
    " - for example, if there is a chocolate candy, you need to output chocolate\n"
    " - another example, if there is mint chewing gum, you need to output mint\n"
    " - if no flavour is to be found for the product, output none\n"
    "7.Classify the product into one of the predefined sub-categories listed below:\n "
    " - rice\n"
    " - pasta (spaghetti)\n"
    " - pasta (macaroni)\n"
    " - pasta (other)\n"
    " - beans/lentils/peas\n"
    " - couscous\n"
    " - mixed vegetables (canned)\n"
    " - peas (canned)\n"
    " - tomatoes (canned)\n"
    " - beans (canned)\n"
    " - corned beef\n"
    " - tuna (canned)\n"
    " - sardines (canned)\n"
    " - soup (canned)\n"
    " - olives (jar)\n"
    " - onions (jar)\n"
    " - pickles (jar)\n"
    " - bouillon (cubes)\n"
    " - spices (general)\n"
    " - flour\n"
    " - sugar\n"
    " - salt\n"
    " - vegetable oil\n"
    " - olive oil\n"
    " - vinegar\n"
    " - tomato paste\n"
    " - milk (liquid)\n"
    " - milk (powdered)\n"
    " - evaporated milk\n"
    " - flavored milk\n"
    " - butter\n"
    " - margarine\n"
    " - cheese\n"
    " - yogurt\n"
    " - chips (potato)\n"
    " - chips (corn)\n"
    " - biscuits/cookies\n"
    " - crackers\n"
    " - candy (hard)\n"
    " - candy (chewy)\n"
    " - chocolate bars\n"
    " - chocolate (powdered)\n"
    " - chocolate spread\n"
    " - chewing gum\n"
    " - snack cakes\n"
    " - nuts\n"
    " - popcorn\n"
    " - breakfast cereal\n"
    " - instant noodles/soup\n"
    " - ketchup\n"
    " - mustard\n"
    " - mayonnaise\n"
    " - hot sauce\n"
    " - soy sauce\n"
    " - fish sauce\n"
    " - water (bottled)\n"
    " - water (bag)\n"
    " - soft drink (carbonated)\n"
    " - energy drink\n"
    " - juice/fruit drink (liquid)\n"
    " - juice/fruit drink (powdered)\n"
    " - tea (bags)\n"
    " - tea (liquid)\n"
    " - coffee (beans/ground)\n"
    " - coffee (instant)\n"
    " - coffee (liquid)\n"
    " - alcoholic drink (beer/cider)\n"
    " - alcoholic drink (wine)\n"
    " - alcoholic drink (spirits)\n"
    " - shampoo\n"
    " - conditioner\n"
    " - body wash\n"
    " - bar soap\n"
    " - hand sanitizer\n"
    " - deodorant\n"
    " - toothpaste\n"
    " - toothbrush\n"
    " - mouthwash\n"
    " - dental floss\n"
    " - sanitary pad/tampon\n"
    " - shaving cream\n"
    " - razors\n"
    " - cotton swabs\n"
    " - cotton balls\n"
    " - tissues\n"
    " - toilet paper\n"
    " - diapers\n"
    " - baby wipes\n"
    " - baby formula\n"
    " - baby cereal\n"
    " - insecticide (spray)\n"
    " - mosquito repellents\n"
    " - basic medicine\n"
    " - body lotion\n"
    " - face cream\n"
    " - sunscreen\n"
    " - lip balm\n"
    " - hair gel\n"
    " - hair spray\n"
    " - hair oil\n"
    " - detergent (powdered)\n"
    " - detergent (liquid)\n"
    " - bleach (powdered)\n"
    " - bleach (liquid)\n"
    " - floor cleaner\n"
    " - window cleaner\n"
    " - dishwashing liquid\n"
    " - dishwashing paste\n"
    " - all-purpose cleaner\n"
    " - napkins\n"
    " - paper towels\n"
    " - aluminium foil\n"
    " - cling wrap\n"
    " - cigarettes\n"
    " - cigarette paper\n"
    " - lighter\n"
    " - matches\n"
    " - batteries\n"
    " - light bulbs\n"
    " - electronics\n"
    "The response should be formatted in JSON, like this:\n"
    "{'class': <class>, 'brand': <brand>, 'product_name': <product_name>, 'material': <material>,"
    " 'quantity_units': <quantity>_<units>, 'flavour': <flavour>, 'subcategory': <subcategory>}"
)


def estimate_cost_gpt(
    prompt_text, response_text, model="gpt-4-128k", image_cost=0.000638, nr_calls=200
):
    pricing = {
        "gpt-4-128k": {"prompt": 0.00250 / 1000, "completion": 0.01000 / 1000},
    }

    enc = tiktoken.encoding_for_model(model)

    # Estimate tokens
    system_prompt_tokens = len(enc.encode(prompt_text["system"]))
    input_prompt_tokens = len(enc.encode(prompt_text["user"]))
    response_tokens = len(enc.encode(response_text))

    total_prompt_tokens = system_prompt_tokens + input_prompt_tokens
    total_completion_tokens = response_tokens

    # Calculate costs
    token_costs = pricing[model]
    prompt_cost = total_prompt_tokens * token_costs["prompt"]
    completion_cost = total_completion_tokens * token_costs["completion"]

    cost = {
        "prompt_tokens": total_prompt_tokens,
        "completion_tokens": total_completion_tokens,
        "image cost": image_cost,
        "total_cost": prompt_cost + completion_cost + image_cost,
    }

    print(f"Prompt Tokens: {cost['prompt_tokens']}")
    print(f"Completion Tokens: {cost['completion_tokens']}")
    print(f"Image Size: maximum 512x512")
    print(f"Total Cost / call: ${cost['total_cost']}")
    print(f"Total cost per {nr_calls} calls: ${cost['total_cost'] * nr_calls}")


def process_image_with_gpt(text_prompt: str, image_path: str):
    """Processes a single image using GPT and returns the result."""
    image_name = os.path.basename(image_path)

    with open(image_path, "rb") as img_file:
        img_b64_str = base64.b64encode(img_file.read()).decode("utf-8")
    img_type = mimetypes.guess_type(image_path)[0]

    try:

        client = OpenAI(
            api_key=os.environ.get(
                "OPENAI_API_KEY"
            ),  # This is the default and can be omitted
        )

        # GPT-4 API call
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": text_prompt},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{img_type};base64,{img_b64_str}"
                            },
                        },
                    ],
                },
            ],
        )

        # Extract and parse the response
        gpt_response = response.choices[0].message.content
        try:
            gpt_response = gpt_response.strip("```json\n").strip("```")
            result_dict = json.loads(gpt_response)
        except json.JSONDecodeError as e:
            print(f"Error parsing GPT response for {image_name}:\n{e}")
            result_dict = {"class": "", "brand": ""}

    except Exception as e:
        print(f"Error in GPT API for {image_name}:\n{e}")
        result_dict = {"class": "", "brand": ""}

    return image_name, result_dict


def process_image_with_qwen(text_prompt: str, image_path: str):
    """Processes a single image and returns the result."""
    image_name = os.path.basename(image_path)
    try:
        client = Client("Qwen/Qwen2-VL")
        try:
            result = client.predict(
                history=[], file=handle_file(image_path), api_name="/add_file"
            )

            result = client.predict(history=[], text=text_prompt, api_name="/add_text")

            result = client.predict(
                _chatbot=[("Run the task!", None)], api_name="/predict"
            )
        except Exception as e:
            print(f"Error in HF Client - {image_name}:\n {e}")
            return image_name, {"class": "", "brand": ""}

        try:
            raw_json = result[0][1].strip("```json\n").strip("```")
            result_dict = json.loads(raw_json)
        except Exception as e:
            print(f"Error in parsing response - {image_name}:\n {e}")
            return image_name, {"class": "", "brand": ""}

    except httpx.ConnectTimeout:
        print("Connection timeout")
        return image_name, {"class": "", "brand": ""}

    return image_name, result_dict


def get_product_data_parallel(
    text_prompt: str,
    images_path: List[str],
    model_function: Tuple[str, Any],
    output_name: str,
    max_workers: int = 4,
):
    """Processes images in parallel."""
    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_image = {
            executor.submit(model_function[1], text_prompt, image_path): image_path
            for image_path in images_path
        }

        for future in tqdm.tqdm(as_completed(future_to_image), total=len(images_path)):
            try:
                image_name, result_dict = future.result()
                results[image_name] = result_dict
                print(f"{image_name}:\n{results[image_name]}")
            except Exception as e:
                print(f"Error processing an image: {e}")

    # Convert results to a DataFrame and save to CSV
    df = (
        pd.DataFrame.from_dict(results, "index")
        .reset_index()
        .rename(columns={"index": "image_name"})
    )
    df.to_csv(f"{output_name}_{model_function[0]}.csv", index=False)


if __name__ == "__main__":
    PROCESSING_MAP = {"qwen": process_image_with_qwen, "gpt-4o": process_image_with_gpt}

    model = "gpt-4o"
    images_path = (
        "C:\\Users\\david\\Downloads\\product_images(20)\\barcode_images_resized"
    )

    all_images = os.listdir(images_path)
    all_images_paths = [os.path.join(images_path, image) for image in all_images]

    get_product_data_parallel(
        USER_PROMPT,
        all_images_paths,
        (model, PROCESSING_MAP[model]),
        "nigerian_products",
        max_workers=10,
    )

    # Example usage for cost estimation for GPT-4o
    NR_CALLS = 4000
    prompt_text = {
        "system": SYSTEM_PROMPT,
        "user": USER_PROMPT,
    }
    response_text = (
        '{"class": "bottle", "brand": "Coke", "product_name": "coca-cola", "material": "plastic",'
        ' "quantity_units": "200_g", "flavour": "chocolate", "subcategory": "carbonated soft drink"}'
    )
    estimate_cost_gpt(prompt_text, response_text, nr_calls=NR_CALLS)
