# conversation_memory_async
import logging
import json
import time
import re
import numpy as np
from typing import Optional, Dict, List, Any, Union, Tuple
from datetime import datetime

# SQLAlchemy Imports
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, JSON, Index, select, func, desc, text
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

# Configure Logging
logger = logging.getLogger(__name__)

class ConversationMemoryError(Exception):
    """Custom exception for conversation memory operations"""
    pass

# SQLAlchemy Base
Base = declarative_base()

# -----------------------------------------------------------------------------
# Database Models (The New Structure)
# -----------------------------------------------------------------------------

class ConversationTurn(Base):
    """Mapped to the existing 'conversation_memory' table."""
    __tablename__ = 'conversation_memory'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(255), index=True, nullable=False)
    session_id = Column(String(255), index=True, nullable=True)
    timestamp = Column(DateTime, default=datetime.now)
    
    # Content
    query_text = Column(Text, nullable=False)
    response_text = Column(Text, nullable=False)
    
    # Metadata
    rag_mode = Column(String(50))
    context_items = Column(JSON) 
    turn_order = Column(Integer, nullable=False)
    memory_log = Column(JSON)

    # Vector Embedding (JSON list of floats)
    query_embedding = Column(JSON, nullable=True)
    
    # Market mode brochure sources (KV Sample)
    market_sources = Column(JSON, nullable=True)

    # LLM thinking/reasoning text (persisted for session reload)
    thinking_text = Column(Text, nullable=True)

    # Index for faster retrieval
    __table_args__ = (
        Index('idx_user_session_order', 'user_id', 'session_id', 'turn_order'),
    )

class ConversationSummary(Base):
    """Mapped to the new 'conversation_summaries' table."""
    __tablename__ = 'conversation_summaries'

    user_id = Column(String(255), primary_key=True)
    session_id = Column(String(255), primary_key=True) 
    summary_text = Column(Text, nullable=False)
    last_summarized_turn_id = Column(Integer) 
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

# -----------------------------------------------------------------------------
# Synchronous Memory Manager (Backward Compatible)
# -----------------------------------------------------------------------------

class ConversationMemory:
    """
    Synchronous Memory Manager using SQLAlchemy.
    Fully compatible with legacy app calls while supporting new Hybrid features.
    """

    def __init__(self, db_path: str = None, max_history_turns=5, pool_size=10, echo_sql=False, **kwargs):
        """
        Args:
            db_path: Path to SQLite database file. If None, uses './data/demo.db'.
        """
        self.max_history_turns = max_history_turns

        # Construct Database URL for SQLite (Portfolio Demo)
        if db_path is None:
            import os
            db_path = os.path.join(os.path.dirname(__file__), 'data', 'demo.db')

        db_url = f"sqlite:///{db_path}"

        # Initialize Synchronous Engine
        self.engine = create_engine(
            db_url,
            pool_size=pool_size,
            pool_recycle=3600,
            pool_pre_ping=True, # Auto-reconnect
            echo=echo_sql
        )
        
        try:
            Base.metadata.create_all(self.engine)
        except Exception as e:
            logger.error(f"Failed to ensure database schema: {e}")

        # Migrate: add thinking_text column if not present
        try:
            with self.engine.connect() as conn:
                conn.execute(text(
                    "ALTER TABLE conversation_memory ADD COLUMN thinking_text TEXT NULL"
                ))
                conn.commit()
                logger.info("Added 'thinking_text' column to conversation_memory.")
        except Exception:
            pass  # Column already exists

        self.SessionLocal = sessionmaker(bind=self.engine)
        logger.info(f"ConversationMemory (Sync) initialized with SQLAlchemy.")

    # -------------------------------------------------------------------------
    # Helper: Vector Math
    # -------------------------------------------------------------------------
    def _cosine_similarity(self, vec_a: List[float], vec_b: List[float]) -> float:
        if not vec_a or not vec_b: return 0.0
        
        if len(vec_a) != len(vec_b):
            return 0.0 

        try:
            a = np.array(vec_a, dtype=float)
            b = np.array(vec_b, dtype=float)
            norm_a = np.linalg.norm(a)
            norm_b = np.linalg.norm(b)
            if norm_a == 0 or norm_b == 0: return 0.0
            return float(np.dot(a, b) / (norm_a * norm_b))
        except Exception:
            return 0.0

    # -------------------------------------------------------------------------
    # Core Operations (Backward Compatible Signatures)
    # -------------------------------------------------------------------------

    def update_context(self, user_id, query_text, response_text, rag_mode, context_items, session_id=None, memory_log=None, query_embedding=None, market_sources=None):
        """
        Updates context. Accepts optional query_embedding and market_sources.
        ENFORCES: query_embedding must be 768 dimensions if provided.
        """
        if query_embedding is not None:
            if len(query_embedding) != 768:
                logger.warning(f"⚠️ Vector dimension mismatch! Expected 768, got {len(query_embedding)}. Dropping vector to prevent DB pollution.")
                query_embedding = None # Do not save invalid vectors

        session = self.SessionLocal()
        try:
            # 1. Get next turn order
            max_order = session.query(func.max(ConversationTurn.turn_order)).filter_by(
                user_id=user_id, session_id=session_id
            ).scalar() or 0
            
            # 2. Create Turn
            new_turn = ConversationTurn(
                user_id=user_id,
                session_id=session_id,
                query_text=query_text,
                response_text=response_text,
                rag_mode=rag_mode,
                context_items=context_items,
                memory_log=memory_log,
                turn_order=max_order + 1,
                query_embedding=query_embedding,
                market_sources=market_sources  # ADD THIS
            )
            
            session.add(new_turn)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to update context: {e}")
            raise ConversationMemoryError(f"DB Error: {e}")
        finally:
            session.close()

    def get_context(self, user_id, session_id=None):
        """
        Retrieves last N turns. Returns dictionary format expected by legacy app.
        """
        session = self.SessionLocal()
        try:
            turns = session.query(ConversationTurn).filter_by(
                user_id=user_id, session_id=session_id
            ).order_by(desc(ConversationTurn.turn_order)).limit(self.max_history_turns).all()
            
            formatted_turns = []
            for t in reversed(turns): # Return chronological
                formatted_turns.append({
                    'query_text': t.query_text,
                    'response_text': t.response_text,
                    'rag_mode': t.rag_mode,
                    'context_items': t.context_items,
                    'timestamp': t.timestamp,
                    'turn_order': t.turn_order,
                    'query_embedding': t.query_embedding
                })
            
            return {'turns': formatted_turns}
        finally:
            session.close()

    def update_summary(self, user_id, summary_text, last_turn_id, session_id=None):
        """
        Updates or creates the conversation summary in the database.
        """
        session = self.SessionLocal()
        try:
            # Ensure session_id is handled (primary key requirement)
            pk_session_id = session_id if session_id else "default"

            # Check if a summary already exists for this user+session
            summary = session.query(ConversationSummary).filter_by(
                user_id=user_id, session_id=pk_session_id
            ).first()

            if summary:
                # Update existing
                summary.summary_text = summary_text
                summary.last_summarized_turn_id = last_turn_id
                summary.updated_at = datetime.now()
            else:
                # Create new
                new_summary = ConversationSummary(
                    user_id=user_id,
                    session_id=pk_session_id,
                    summary_text=summary_text,
                    last_summarized_turn_id=last_turn_id
                )
                session.add(new_summary)

            session.commit()
            logger.info(f"Successfully updated summary for User {user_id} Session {session_id}")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to update summary: {e}")
            raise ConversationMemoryError(f"DB Error: {e}")
        finally:
            session.close()

    # -------------------------------------------------------------------------
    # Restored clear_cache Method
    # -------------------------------------------------------------------------
    def clear_cache(self, user_id, session_id):
        """
        Deletes conversation history and summaries for a specific session.
        Called by the API endpoint after raw SQL deletion.
        """
        session = self.SessionLocal()
        try:
            # 1. Delete Summary (The API raw SQL misses this table)
            # Handle default session mapping logic
            pk_session_id = session_id if session_id else "default"
            
            session.query(ConversationSummary).filter_by(
                user_id=user_id, 
                session_id=pk_session_id
            ).delete()

            # 2. Delete Turns (Redundant if API does it, but ensures consistency)
            session.query(ConversationTurn).filter_by(
                user_id=user_id, 
                session_id=session_id
            ).delete()
            
            session.commit()
            logger.info(f"Cleared memory cache (turns & summary) for user {user_id}, session {session_id}")
        except Exception as e:
            session.rollback()
            logger.error(f"Error clearing cache: {e}")
            # We don't raise here to avoid breaking the API response if the DB is slightly off
        finally:
            session.close()

    # -------------------------------------------------------------------------
    # Restored Helper Methods (Re-implemented with SQLAlchemy)
    # -------------------------------------------------------------------------

    def get_hybrid_history_for_prompt(self, user_id, session_id=None, turn_threshold=3, char_threshold=15000):
        """
        Re-implementation of the string formatting logic for LLM prompts.
        """
        context_data = self.get_context(user_id, session_id)
        if not context_data or not context_data.get('turns'):
            return ""

        all_turns = context_data['turns']
        # Take the most recent N turns
        relevant_turns = all_turns[-turn_threshold:] if turn_threshold > 0 else all_turns

        history_parts = []
        current_char_count = 0

        for turn in reversed(relevant_turns):
            q = turn.get('query_text', '')
            raw_a = turn.get('response_text', "")
            
            # Basic HTML stripping
            clean_a = re.sub(r'<[^>]+>', ' ', raw_a)
            clean_a = re.sub(r'\s+', ' ', clean_a).strip()
            
            turn_str = f"User: {q}\nAssistant: {clean_a}\n"
            
            if current_char_count + len(turn_str) > char_threshold:
                break
            
            history_parts.insert(0, turn_str)
            current_char_count += len(turn_str)

        return "\n".join(history_parts) if history_parts else ""

    def get_last_query_history_str(self, user_id, session_id=None, num_turns_for_prompt=3):
        """
        Constructs string representation of last few turns.
        """
        context_data = self.get_context(user_id, session_id)
        if not context_data or not context_data.get('turns'):
            return "(Tidak ada riwayat percakapan)"

        all_turns = context_data['turns']
        start_index = max(0, len(all_turns) - num_turns_for_prompt)
        relevant_turns = all_turns[start_index:]

        history_parts = []
        for turn in relevant_turns:
            q = turn.get('query_text', '')
            a = turn.get('response_text', "(Processing...)")
            # Simple strip for this view
            clean_a = re.sub(r'<[^>]+>', ' ', a).strip()[:200] + "..." if len(a) > 200 else a
            history_parts.append(f"User: {q}\nAssistant: {clean_a}")
        
        return "\n".join(history_parts) if history_parts else "(Tidak ada riwayat percakapan)"

    def get_last_providers_used(self, user_id, session_id=None):
        """
        Finds the last context items where rag_mode was 'market'.
        """
        session = self.SessionLocal()
        try:
            turn = session.query(ConversationTurn).filter_by(
                user_id=user_id, session_id=session_id, rag_mode='market'
            ).order_by(desc(ConversationTurn.turn_order)).first()
            
            if turn and turn.context_items:
                return turn.context_items
            return None
        finally:
            session.close()

    def get_recent_documents(self, user_id, session_id=None, max_docs_to_return=3):
        """
        Finds unique documents from recent 'document' mode turns.
        """
        session = self.SessionLocal()
        try:
            # Fetch last 10 turns to scan for docs
            turns = session.query(ConversationTurn).filter_by(
                user_id=user_id, session_id=session_id, rag_mode='document'
            ).order_by(desc(ConversationTurn.turn_order)).limit(10).all()
            
            unique_docs = {}
            for turn in turns:
                if not turn.context_items or not isinstance(turn.context_items, list):
                    continue
                
                for item in turn.context_items:
                    if isinstance(item, dict) and 'path' in item:
                        if item['path'] not in unique_docs:
                            unique_docs[item['path']] = item
                            if len(unique_docs) >= max_docs_to_return:
                                break
                if len(unique_docs) >= max_docs_to_return:
                    break
            
            return list(unique_docs.values())
        finally:
            session.close()

    # -------------------------------------------------------------------------
    # NEW: Hybrid Retrieval (Exposed Synchronously)
    # -------------------------------------------------------------------------

    def get_smart_context(self, user_id, session_id=None, current_query_embedding=None, recent_k=3, relevant_k=2):
        """
        New Hybrid retrieval. Can be called by your app when you are ready to pass embeddings.
        """
        session = self.SessionLocal()
        pk_session_id = session_id if session_id else "default"
        
        try:
            # 1. Summary
            summary = session.query(ConversationSummary).filter_by(
                user_id=user_id, session_id=pk_session_id
            ).first()
            summary_text = summary.summary_text if summary else ""

            # 2. Fetch Turns
            all_turns = session.query(ConversationTurn).filter_by(
                user_id=user_id, session_id=session_id
            ).order_by(ConversationTurn.turn_order.asc()).all()

            if not all_turns:
                return {"summary": "", "turns": []}

            # 3. Recent
            recent_turns = all_turns[-recent_k:] if len(all_turns) > recent_k else all_turns
            recent_ids = {t.id for t in recent_turns}

            # 4. Relevant (Vector)
            relevant_turns = []
            if current_query_embedding and len(all_turns) > recent_k:
                older_turns = [t for t in all_turns if t.id not in recent_ids]
                older_turns = older_turns[-50:]  # cap scan to avoid O(n) cost on long histories
                scored = []
                for t in older_turns:
                    if t.query_embedding:
                        # Handle JSON decoding if driver returns string
                        emb = t.query_embedding
                        if isinstance(emb, str):
                            try: emb = json.loads(emb)
                            except: continue
                        
                        score = self._cosine_similarity(current_query_embedding, emb)
                        if score > 0.7:
                            scored.append((score, t))
                
                scored.sort(key=lambda x: x[0], reverse=True)
                relevant_turns = [x[1] for x in scored[:relevant_k]]

            # 5. Merge
            final_turns = sorted(list(recent_turns) + list(relevant_turns), key=lambda x: x.turn_order)
            
            # Format
            formatted = []
            for t in final_turns:
                formatted.append({"role": "user", "content": t.query_text})
                formatted.append({"role": "assistant", "content": t.response_text})

            return {"summary": summary_text, "turns": formatted}
            
        finally:
            session.close()

    def insert_user_turn(self, user_id, session_id, query_text):
        """
        Phase 1: Insert user query immediately with a placeholder response.
        Returns the primary key (id) of the new row.
        """
        session = self.SessionLocal()
        try:
            # 1. Calculate turn order
            max_order = session.query(func.max(ConversationTurn.turn_order)).filter_by(
                user_id=user_id, session_id=session_id
            ).scalar() or 0
            
            # 2. Create Turn with placeholder response (required by nullable=False)
            new_turn = ConversationTurn(
                user_id=user_id,
                session_id=session_id,
                query_text=query_text,
                response_text="[Generating...]", # Placeholder
                turn_order=max_order + 1,
                timestamp=datetime.now()
            )
            
            session.add(new_turn)
            session.commit()
            
            # 3. Refresh to get the auto-generated ID
            session.refresh(new_turn)
            return new_turn.id
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to insert user turn: {e}")
            # Raise so the app knows it failed to save
            raise ConversationMemoryError(f"DB Error: {e}")
        finally:
            session.close()

    def update_bot_response(self, turn_id, response_text, rag_mode=None, context_items=None, memory_log=None, query_embedding=None, market_sources=None, thinking_text=None):
        """
        Phase 2: Update the existing turn with the final AI response and metadata.
        """
        session = self.SessionLocal()
        try:
            # 1. Fetch the specific turn
            turn = session.query(ConversationTurn).get(turn_id)
            if not turn:
                logger.error(f"Turn ID {turn_id} not found for update.")
                return

            # 2. Update fields
            turn.response_text = response_text
            
            # Update optional metadata if provided
            if rag_mode is not None:
                turn.rag_mode = rag_mode
            if context_items is not None:
                turn.context_items = context_items
            if memory_log is not None:
                turn.memory_log = memory_log
            if market_sources is not None:
                turn.market_sources = market_sources
            if thinking_text is not None:
                turn.thinking_text = thinking_text

            # Handle Vector Embedding with validation
            if query_embedding is not None:
                if len(query_embedding) != 768:
                    logger.warning(f"⚠️ Vector dimension mismatch in update! Expected 768, got {len(query_embedding)}. Dropping.")
                else:
                    turn.query_embedding = query_embedding

            session.commit()
            logger.info(f"Successfully updated Bot response for Turn ID {turn_id}")

        except Exception as e:
            session.rollback()
            logger.error(f"Failed to update bot response: {e}")
        finally:
            session.close()

    def cleanup(self):
        self.engine.dispose()