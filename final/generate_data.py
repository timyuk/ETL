from pymongo import MongoClient
import random
from datetime import datetime, timedelta

client = MongoClient("mongodb://root:root@localhost:27017/")
db = client["etl_db"]

db.UserSessions.delete_many({})
db.EventLogs.delete_many({})
db.SupportTickets.delete_many({})
db.UserRecommendations.delete_many({})
db.ModerationQueue.delete_many({})



users = random.sample([f"user_{i}" for i in range(0, 50)], 5)
products = random.sample([f"prod_{i}" for i in range(1, 200)], 20)


for i in range(20):
    user = random.choice(users)
    start = datetime(2026, 2, random.randint(1, 28))
    end = start + timedelta(minutes=random.randint(1, 120))
    pages_visited = random.sample(
            ["/home", "/products", "/cart", "/profile"],
            k=random.randint(1, 4)
        )
    device = random.choice(["mobile", "desktop"])
    actions = random.sample(
            ["login","logout", "search"],
            k=random.randint(1, 3)
        )
    if "/products" in pages_visited:
        pages_visited.append(f"/products/{random.randint(1, 100)}")
        actions.append("view_product")
        if random.random() > 0.5:
            actions.append("add_to_cart")
    if "/cart" in pages_visited and random.random() > 0.5:
        actions.append("checkout")


    db.UserSessions.insert_one({
        "session_id": f"sess_{random.randint(1,100)}",
        "user_id": user,
        "start_time": start,
        "end_time": end,
        "pages_visited": pages_visited,
        "device": device,
        "actions": actions
    })

    session_duration = end-start

    for i in range(random.randint(1, 5)):
        db.EventLogs.insert_one({
            "event_id": f"evt_{random.randint(1,100)}",
            "timestamp": start + timedelta(seconds=random.randint(1, int(session_duration.total_seconds()))),
            "event_type": random.choice(["click", "view", "scroll"]),
            "details": random.choice(pages_visited)
        })

problems = ["Не могу оплатить заказ", "Не могу оформить заказ", "Не могу отменить заказ", "Помогите", "Пожалуйста, помогите"]
solutions = ["Пожалуйста, уточните способ оплаты", "Пожалуйста, уточните номер заказа", "Скоро вам ответим", "К сожалению у нас выходной", "Попробуйте заново"]
for i in range(15):
    created = datetime(2026, 2, random.randint(1, 25))
    messages = [{
                "sender": "user",
                "message": random.choice(problems),
                "timestamp": created
            }]
    last_updated = created
    for j in range(random.randint(1, 4)):
        updated = last_updated + timedelta(minutes=random.randint(30, 300))
        sender = random.choice(["user", "support"])
        messages.append({
            "sender": sender,
            "message": random.choice(problems) if sender == "user" else random.choice(solutions),
            "timestamp": updated
        })
        last_updated = updated

    db.SupportTickets.insert_one({
        "ticket_id": f"ticket_{random.randint(1,100)}",
        "user_id": random.choice(users),
        "status": random.choice(["open", "closed", "in_progress"]),
        "issue_type": random.choice(["payment", "delivery", "account"]),
        "messages": messages,
        "created_at": created,
        "updated_at": last_updated
    })


for user in users:
    start = datetime(2026, 2, random.randint(1, 28))
    last_updated = start + timedelta(minutes=random.randint(1, 120))
    db.UserRecommendations.insert_one({
        "user_id": user,
        "recommended_products": random.sample(products, k=random.randint(1, 5)),
        "last_updated": last_updated
    })

reviews = ["Отлично", "Супер", "Очень удобно", "Слишком дорого", "Ужасно", "Куплю еще раз"]
for i in range(20):
    start = datetime(2026, 2, random.randint(1, 28))
    submitted_at = start + timedelta(minutes=random.randint(1, 120))
    db.ModerationQueue.insert_one({
        "review_id": f"rev_{random.randint(1,100)}",
        "user_id": random.choice(users),
        "product_id": random.choice(products),
        "review_text": random.choice(reviews),
        "rating": random.randint(1, 5),
        "moderation_status": random.choice(["pending", "approved", "rejected"]),
        "flags": random.sample(
            ["contains_images", "spam", "offensive"],
            k=random.randint(0, 2)
        ),
        "submitted_at": submitted_at
    })

