"""
RAG Pipeline
Generates business insights from mart tables,
embeds them into ChromaDB vector store,
and answers questions using retrieval + Llama.

Run:
    python ai/rag_pipeline.py
"""

import os
import sys
from pathlib import Path

import chromadb
from groq import Groq
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent.parent))
from notebooks.athena_helper import query

CHROMA_DIR = "ai/chroma_db"


# ── Generate insights from Athena ─────────────────────────────────────────────

def generate_insights() -> list[dict]:
    """Pull key metrics from Athena and format as text documents."""
    insights = []

    # 1. Overall KPIs
    df = query("""
        select
            count(distinct order_id)            as total_orders,
            count(distinct customer_unique_id)  as total_customers,
            round(sum(order_value), 2)          as total_revenue,
            round(avg(order_value), 2)          as avg_order_value,
            round(avg(review_score), 2)         as avg_review_score,
            round(sum(case when delivered_on_time then 1 else 0 end) * 100.0
                / count(*), 2)                  as on_time_pct
        from dbt_dev.fct_orders
        where status = 'delivered'
    """)
    r = df.iloc[0]
    insights.append({
        "id": "kpi_overview",
        "text": f"""Overall Business KPIs:
        Total delivered orders: {r['total_orders']}
        Total unique customers: {r['total_customers']}
        Total revenue: R$ {r['total_revenue']}
        Average order value: R$ {r['avg_order_value']}
        Average review score: {r['avg_review_score']} out of 5
        On-time delivery rate: {r['on_time_pct']}%""",
    })

    # 2. Customer segments
    df = query("""
        select
            customer_segment,
            count(*)                        as customers,
            round(avg(total_revenue), 2)    as avg_revenue,
            round(avg(total_orders), 2)     as avg_orders
        from dbt_dev.dim_customers
        group by customer_segment
        order by avg_revenue desc
    """)
    seg_text = "Customer Segments:\n"
    for _, row in df.iterrows():
        seg_text += f"  {row['customer_segment']}: {row['customers']} customers, avg revenue R$ {row['avg_revenue']}, avg {row['avg_orders']} orders\n"
    insights.append({"id": "customer_segments", "text": seg_text})

    # 3. Top categories
    df = query("""
        select
            product_category,
            count(distinct order_id)    as orders,
            round(sum(total_amount), 2) as revenue
        from dbt_dev.int_order_items_enriched
        group by product_category
        order by revenue desc
        limit 10
    """)
    cat_text = "Top 10 Product Categories by Revenue:\n"
    for _, row in df.iterrows():
        cat_text += f"  {row['product_category']}: R$ {row['revenue']} from {row['orders']} orders\n"
    insights.append({"id": "top_categories", "text": cat_text})

    # 4. Payment types
    df = query("""
        select
            primary_payment_type,
            count(*)                        as orders,
            round(avg(order_value), 2)      as avg_order_value
        from dbt_dev.fct_orders
        group by primary_payment_type
        order by orders desc
    """)
    pay_text = "Payment Type Breakdown:\n"
    for _, row in df.iterrows():
        pay_text += f"  {row['primary_payment_type']}: {row['orders']} orders, avg R$ {row['avg_order_value']}\n"
    insights.append({"id": "payment_types", "text": pay_text})

    # 5. State performance
    df = query("""
        select
            customer_state,
            count(distinct order_id)        as orders,
            round(sum(order_value), 2)      as revenue
        from dbt_dev.fct_orders
        where status = 'delivered'
        group by customer_state
        order by revenue desc
        limit 10
    """)
    state_text = "Top 10 States by Revenue:\n"
    for _, row in df.iterrows():
        state_text += f"  {row['customer_state']}: R$ {row['revenue']} from {row['orders']} orders\n"
    insights.append({"id": "state_performance", "text": state_text})

    # 6. Churn stats
    df = query("""
        select
            is_churned,
            count(*)                        as customers,
            round(avg(total_revenue), 2)    as avg_revenue
        from dbt_dev.mart_customer_metrics
        group by is_churned
    """)
    churn_text = "Churn Analysis:\n"
    for _, row in df.iterrows():
        status = "Churned" if row['is_churned'] == 1 else "Active"
        churn_text += f"  {status}: {row['customers']} customers, avg revenue R$ {row['avg_revenue']}\n"
    insights.append({"id": "churn_stats", "text": churn_text})

    print(f"  Generated {len(insights)} insight documents")
    return insights


# ── Build vector store ────────────────────────────────────────────────────────

def simple_embed(texts: list[str]) -> list[list[float]]:
    """Simple hash-based embedding — no model download needed."""
    import hashlib
    embeddings = []
    for text in texts:
        vec = []
        for i in range(64):
            h = int(hashlib.md5(f"{text}{i}".encode()).hexdigest(), 16)
            vec.append((h % 10000) / 10000.0)
        embeddings.append(vec)
    return embeddings


def build_vector_store(insights: list[dict]) -> chromadb.Collection:
    """Store insights in ChromaDB using simple embeddings."""
    client = chromadb.PersistentClient(path=CHROMA_DIR)

    try:
        client.delete_collection("insights")
    except Exception:
        pass

    collection = client.create_collection(
        "insights",
        metadata={"hnsw:space": "cosine"}
    )

    texts      = [i["text"] for i in insights]
    embeddings = simple_embed(texts)

    collection.add(
        ids        = [i["id"] for i in insights],
        documents  = texts,
        embeddings = embeddings,
    )

    print(f"  Stored {len(insights)} documents in ChromaDB")
    return collection


def get_vector_store() -> chromadb.Collection:
    """Load existing ChromaDB collection."""
    client = chromadb.PersistentClient(path=CHROMA_DIR)
    return client.get_collection(
        "insights",
    )


# ── RAG Query ─────────────────────────────────────────────────────────────────

def rag_query(question: str, collection: chromadb.Collection) -> str:
    """Retrieve relevant context and answer using Llama."""

    # retrieve top 3 relevant documents using simple embeddings
    query_embedding = simple_embed([question])[0]
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=3,
    )
    context = "\n\n".join(results["documents"][0])

    # generate answer with Llama
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {
                "role": "system",
                "content": """You are a helpful business analyst assistant.
                Answer questions based on the provided data context.
                Be concise, specific, and use numbers from the context.
                If the answer is not in the context, say so."""
            },
            {
                "role": "user",
                "content": f"Context:\n{context}\n\nQuestion: {question}"
            }
        ],
        temperature=0.1,
        max_tokens=500,
    )

    return response.choices[0].message.content.strip()


# ── Main ──────────────────────────────────────────────────────────────────────

DEMO_QUESTIONS = [
    "What is our total revenue and how many customers do we have?",
    "Which customer segment generates the most revenue?",
    "What are our top performing product categories?",
    "How is our on-time delivery performance?",
    "What percentage of customers have churned?",
]


if __name__ == "__main__":
    print("=" * 55)
    print("  RAG Pipeline — Powered by Llama 3.1 on Groq")
    print("=" * 55)

    # build knowledge base
    print("\n📚 Building knowledge base from Athena...")
    insights   = generate_insights()
    collection = build_vector_store(insights)

    # demo questions
    print("\n" + "=" * 55)
    print("  Demo Questions")
    print("=" * 55)

    for question in DEMO_QUESTIONS:
        print(f"\n❓ {question}")
        print("-" * 50)
        answer = rag_query(question, collection)
        print(f"💡 {answer}")

    # interactive mode
    print("\n" + "=" * 55)
    print("  Interactive Mode — type 'exit' to quit")
    print("=" * 55)

    while True:
        question = input("\n❓ Your question: ").strip()
        if question.lower() in ("exit", "quit", "q"):
            break
        if question:
            answer = rag_query(question, collection)
            print(f"\n💡 {answer}")