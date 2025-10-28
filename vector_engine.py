import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

from config import settings

logger = logging.getLogger(__name__)


class VectorEngine:
    """
    Manages embeddings and semantic search using Chroma.
    Stores unified Mendix entity documents in a single collection for cross-entity search.
    """

    def __init__(self):
        self.enabled = settings.enable_vector_search
        self.client = None
        self.collection = None
        if self.enabled:
            try:
                import chromadb
                from chromadb.config import Settings as ChromaSettings
                self.client = chromadb.PersistentClient(
                    path=settings.vector_db_path,
                    settings=ChromaSettings(anonymized_telemetry=False),
                )
                self.collection = self.client.get_or_create_collection(
                    name="mendix",
                )
                logger.info("VectorEngine initialized with Chroma collection 'mendix'")
            except Exception as e:
                logger.error(f"Failed to initialize Chroma: {e}")
                self.enabled = False

    def _embed_texts(self, texts: List[str]) -> List[List[float]]:
        """Use OpenAI embeddings to vectorize texts."""
        try:
            from openai import OpenAI
            client = OpenAI(api_key=settings.openai_api_key)
            resp = client.embeddings.create(
                model=settings.openai_embedding_model,
                input=texts,
            )
            return [d.embedding for d in resp.data]
        except Exception as e:
            logger.error(f"Embedding error: {e}")
            return []

    def upsert_all(self, db: Session) -> int:
        """
        Build or refresh embeddings for key textual fields across entities.
        Returns the number of documents upserted.
        """
        if not self.enabled or self.collection is None:
            logger.info("VectorEngine disabled or not initialized; skipping upsert_all")
            return 0

        from models import Employee, Goal, Project, Skill

        docs: List[str] = []
        ids: List[str] = []
        metadatas: List[Dict[str, Any]] = []

        # Employees: name + designation + email
        for e in db.query(Employee).all():
            text = f"Employee: {e.full_name} | {e.designation or ''} | {e.email}"
            docs.append(text)
            ids.append(f"employee:{e.employee_id}")
            metadatas.append({"type": "employee", "employee_id": e.employee_id, "full_name": e.full_name})

        # Goals: title + description + status + category
        for g in db.query(Goal).all():
            text = f"Goal: {g.title} | {g.description or ''} | {g.status} | {g.category or ''}"
            docs.append(text)
            ids.append(f"goal:{g.goal_id}")
            metadatas.append({"type": "goal", "goal_id": g.goal_id, "employee_id": g.employee_id, "status": g.status})

        # Projects: name + description + manager
        for p in db.query(Project).all():
            text = f"Project: {p.name} | {p.description or ''} | Manager: {p.project_manager or ''}"
            ids.append(f"project:{p.project_id}")
            docs.append(text)
            metadatas.append({"type": "project", "project_id": p.project_id, "name": p.name})

        # Skills: name + category + description
        for s in db.query(Skill).all():
            text = f"Skill: {s.name} | {s.category or ''} | {s.description or ''}"
            ids.append(f"skill:{s.skill_id}")
            docs.append(text)
            metadatas.append({"type": "skill", "skill_id": s.skill_id, "name": s.name})

        if not docs:
            logger.info("No documents found to embed.")
            return 0

        embeddings = self._embed_texts(docs)
        if not embeddings or len(embeddings) != len(docs):
            logger.error("Embedding generation failed or mismatched length.")
            return 0

        # Upsert in batches to avoid memory spikes
        batch_size = 128
        total = 0
        for i in range(0, len(docs), batch_size):
            b_docs = docs[i:i+batch_size]
            b_ids = ids[i:i+batch_size]
            b_metas = metadatas[i:i+batch_size]
            b_embeds = embeddings[i:i+batch_size]
            try:
                self.collection.upsert(
                    ids=b_ids,
                    documents=b_docs,
                    metadatas=b_metas,
                    embeddings=b_embeds,
                )
                total += len(b_docs)
            except Exception as e:
                logger.error(f"Chroma upsert batch failed: {e}")
                continue

        logger.info(f"VectorEngine upserted {total} documents")
        return total

    def search(self, query: str, top_k: int = 10) -> List[Dict[str, Any]]:
        """Semantic search across unified collection."""
        if not self.enabled or self.collection is None:
            return []
        try:
            q_embeds = self._embed_texts([query])
            if not q_embeds:
                return []
            res = self.collection.query(
                query_embeddings=q_embeds,
                n_results=top_k,
                include=['metadatas', 'documents', 'distances'],
            )
            results: List[Dict[str, Any]] = []
            for i in range(len(res.get('ids', [[]])[0])):
                results.append({
                    "id": res['ids'][0][i],
                    "text": res['documents'][0][i],
                    "metadata": res['metadatas'][0][i],
                    "score": res['distances'][0][i],
                })
            return results
        except Exception as e:
            logger.error(f"Chroma query failed: {e}")
            return []