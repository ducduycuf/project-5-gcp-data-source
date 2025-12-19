import time, random
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from playwright.sync_api import sync_playwright, TimeoutError

# ======================
# CONFIG
# ======================
MONGO_URI = "mongodb://35.187.241.9:27017"
DB_NAME = "glamiradb"

# 👉 đổi theo VM
TASKS_COL = "task6_crawl_tasks_vm7"
RESULTS_COL = "task6_results"

BASE_URL = "https://www.glamira.com/catalog/product/view/id/{}"

BATCH_SIZE = 50
MAX_RETRY = 2

REACT_FIELDS = [
    "product_id",
    "name",
    "sku",
    "price",
    "category",
    "category_name",
    "store_code",
    "gender",
]

# ======================
def human_delay(success=True):
    if success:
        time.sleep(random.uniform(1.5, 2.8))
    else:
        time.sleep(random.uniform(4.5, 7.0))


def setup_route(context):
    context.route(
        "**/*",
        lambda route, request: (
            route.abort()
            if request.resource_type in ["image", "font", "media"]
            else route.continue_()
        )
    )


# ======================
def load_tasks():
    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][TASKS_COL]

    tasks = list(
        col.find(
            {"product_id": {"$ne": None}},
            {"_id": 0, "product_id": 1}
        )
    )

    print(f"TOTAL TASKS: {len(tasks)}")
    return tasks


def save_batch(rows):
    if not rows:
        return

    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][RESULTS_COL]

    ops = [
        UpdateOne(
            {"product_id": r["product_id"]},
            {"$set": r},
            upsert=True,
        )
        for r in rows
    ]

    col.bulk_write(ops, ordered=False)


# ======================
def main():
    tasks = load_tasks()
    total = len(tasks)

    if total == 0:
        print("NO TASKS FOUND — EXIT")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            locale="en-US",
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )

        setup_route(context)
        page = context.new_page()

        # Warm-up
        page.goto("https://www.glamira.com/", timeout=20000)
        time.sleep(4)

        batch = []

        for idx, task in enumerate(tasks, 1):
            pid = str(task["product_id"])
            url = BASE_URL.format(pid)

            print(f"{idx}/{total} | product_id={pid}")

            result = {
                "product_id": pid,
                "url": url,
                "status": "FAILED",
                "react_fields": None,
                "error": None,
                "crawled_at": datetime.utcnow(),
            }

            success = False

            for _ in range(MAX_RETRY):
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=25000)

                    page.wait_for_function(
                        "() => window.react_data !== undefined",
                        timeout=8000
                    )

                    react_data = page.evaluate("() => window.react_data")

                    if not react_data:
                        raise Exception("react_data empty")

                    result["react_fields"] = {
                        k: react_data.get(k) for k in REACT_FIELDS
                    }
                    result["status"] = "OK"
                    success = True
                    break

                except TimeoutError:
                    result["error"] = "Timeout"
                except Exception as e:
                    result["error"] = str(e)

                human_delay(success=False)

            human_delay(success)
            batch.append(result)

            if len(batch) >= BATCH_SIZE:
                save_batch(batch)
                batch.clear()

        if batch:
            save_batch(batch)

        browser.close()

    print("TASK FINISHED")


if __name__ == "__main__":
    main()
