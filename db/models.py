from __future__ import annotations

import enum
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Post(Base):
    __tablename__ = "posts"
    __table_args__ = (
        Index("idx_posts_post_id", "post_id"),
        Index("idx_posts_published_at", "published_at"),
    )

    id          : Mapped[int]      = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    channel_id  : Mapped[str]      = mapped_column(String(100), nullable=False)
    post_id     : Mapped[str]      = mapped_column(String(255), nullable=False, unique=True)
    content     : Mapped[str]      = mapped_column(Text, nullable=False)
    published_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    collected_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=text("NOW()"))

    analyses: Mapped[list[Analysis]] = relationship("Analysis", back_populates="post", cascade="all, delete-orphan")


class Analysis(Base):
    __tablename__ = "analyses"
    __table_args__ = (
        CheckConstraint("signal_type IN ('BUY', 'SELL', 'HOLD')", name="analyses_signal_type_check"),
        Index("idx_analyses_post_id", "post_id"),
        Index("idx_analyses_signal_type", "signal_type"),
        Index("idx_analyses_created_at", "created_at"),
    )

    id           : Mapped[int]           = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    post_id      : Mapped[int]           = mapped_column(BigInteger, ForeignKey("posts.id", ondelete="CASCADE"), nullable=False)
    signal_type  : Mapped[str]           = mapped_column(String(10), nullable=False)
    scenario_json: Mapped[Any]           = mapped_column(JSONB, nullable=False)
    summary      : Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    invalidation : Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_response : Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at   : Mapped[datetime]      = mapped_column(DateTime, nullable=False, server_default=text("NOW()"))

    post  : Mapped[Post]        = relationship("Post", back_populates="analyses")
    trades: Mapped[list[Trade]] = relationship("Trade", back_populates="analysis")


class Trade(Base):
    __tablename__ = "trades"
    __table_args__ = (
        CheckConstraint("side IN ('BUY', 'SELL')", name="trades_side_check"),
        CheckConstraint("status IN ('PENDING', 'FILLED', 'FAILED', 'CANCELLED')", name="trades_status_check"),
        CheckConstraint("mode IN ('FULL_AUTO', 'SEMI_AUTO')", name="trades_mode_check"),
        Index("idx_trades_analysis_id", "analysis_id"),
        Index("idx_trades_status", "status"),
        Index("idx_trades_executed_at", "executed_at"),
    )

    id             : Mapped[int]               = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    analysis_id    : Mapped[int]               = mapped_column(BigInteger, ForeignKey("analyses.id"), nullable=False)
    symbol         : Mapped[str]               = mapped_column(String(20), nullable=False)
    side           : Mapped[str]               = mapped_column(String(10), nullable=False)
    qty            : Mapped[Decimal]           = mapped_column(Numeric(18, 8), nullable=False)
    price          : Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    status         : Mapped[str]               = mapped_column(String(20), nullable=False, server_default=text("'PENDING'"))
    bybit_order_id : Mapped[Optional[str]]     = mapped_column(String(100), nullable=True)
    stop_loss_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    mode           : Mapped[str]               = mapped_column(String(20), nullable=False)
    executed_at    : Mapped[Optional[datetime]]= mapped_column(DateTime, nullable=True)

    analysis: Mapped[Analysis] = relationship("Analysis", back_populates="trades")


class Settings(Base):
    __tablename__ = "settings"
    __table_args__ = (
        CheckConstraint("mode IN ('FULL_AUTO', 'SEMI_AUTO')", name="settings_mode_check"),
        CheckConstraint("id = 1", name="settings_single_row"),
    )

    id                  : Mapped[int]     = mapped_column(Integer, primary_key=True, default=1)
    mode                : Mapped[str]     = mapped_column(String(20), nullable=False, server_default=text("'SEMI_AUTO'"))
    max_trade_amount_krw: Mapped[int]     = mapped_column(Integer, nullable=False, server_default=text("100000"))
    daily_loss_limit_krw: Mapped[int]     = mapped_column(Integer, nullable=False, server_default=text("300000"))
    stop_loss_pct       : Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False, server_default=text("0.03"))
    is_halted           : Mapped[bool]    = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    updated_at          : Mapped[datetime]= mapped_column(DateTime, nullable=False, server_default=text("NOW()"))


class DailyStat(Base):
    __tablename__ = "daily_stats"
    __table_args__ = (
        UniqueConstraint("date", name="daily_stats_date_key"),
        Index("idx_daily_stats_date", "date"),
    )

    id              : Mapped[int]  = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    date            : Mapped[date] = mapped_column(Date, nullable=False, unique=True)
    total_trades    : Mapped[int]  = mapped_column(Integer, nullable=False, server_default=text("0"))
    realized_pnl_krw: Mapped[int]  = mapped_column(Integer, nullable=False, server_default=text("0"))
    is_halted       : Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
