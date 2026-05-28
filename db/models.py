from __future__ import annotations

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
    SmallInteger,
    String,
    Text,
    Time,
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
        CheckConstraint("post_type IN ('text', 'video', 'mixed')", name="posts_post_type_check"),
        Index("idx_posts_post_id", "post_id"),
        Index("idx_posts_published_at", "published_at"),
        Index("idx_posts_post_type", "post_type"),
    )

    id           : Mapped[int]           = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    channel_id   : Mapped[str]           = mapped_column(String(100), nullable=False)
    post_id      : Mapped[str]           = mapped_column(String(255), nullable=False, unique=True)
    content      : Mapped[str]           = mapped_column(Text, nullable=False)
    post_type    : Mapped[str]           = mapped_column(String(10), nullable=False, server_default=text("'text'"))
    image_urls   : Mapped[Any]           = mapped_column(JSONB, nullable=False, server_default=text("'[]'"))
    content_hash : Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    published_at : Mapped[datetime]      = mapped_column(DateTime, nullable=False)
    collected_at : Mapped[datetime]      = mapped_column(DateTime, nullable=False, server_default=text("NOW()"))

    analyses   : Mapped[list[Analysis]]   = relationship("Analysis", back_populates="post", cascade="all, delete-orphan")
    links      : Mapped[list[PostLink]]   = relationship("PostLink", back_populates="post", cascade="all, delete-orphan")
    video_memos: Mapped[list[VideoMemo]]  = relationship("VideoMemo", back_populates="post")


class Analysis(Base):
    __tablename__ = "analyses"
    __table_args__ = (
        CheckConstraint("signal_type IN ('BUY', 'SELL', 'HOLD')", name="analyses_signal_type_check"),
        Index("idx_analyses_post_id", "post_id"),
        Index("idx_analyses_signal_type", "signal_type"),
        Index("idx_analyses_coin_symbol", "coin_symbol"),
        Index("idx_analyses_created_at", "created_at"),
    )

    id                 : Mapped[int]               = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    post_id            : Mapped[int]               = mapped_column(BigInteger, ForeignKey("posts.id", ondelete="CASCADE"), nullable=False)
    signal_type        : Mapped[str]               = mapped_column(String(10), nullable=False)
    coin_symbol        : Mapped[Optional[str]]     = mapped_column(String(20), nullable=True)
    timeframe          : Mapped[Optional[str]]     = mapped_column(String(10), nullable=True)
    is_reference_only  : Mapped[bool]              = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    youtuber_zone_low  : Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    youtuber_zone_high : Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    entry_price_1      : Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)  # 안정형
    entry_price_2      : Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)  # 중립형
    entry_price_3      : Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)  # 공격형
    entry_price_4      : Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)  # 초공격형
    absolute_stop      : Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)  # 마지노선
    stop_loss_price    : Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    take_profit_price  : Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    short_entry_price  : Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    short_stop_loss    : Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    risk_reward_ratio  : Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    current_rsi        : Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2), nullable=True)
    rsi_signal         : Mapped[Optional[str]]     = mapped_column(String(10), nullable=True)
    volume_signal      : Mapped[Optional[str]]     = mapped_column(String(10), nullable=True)
    fib_level          : Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 3), nullable=True)
    scenario_json      : Mapped[Any]               = mapped_column(JSONB, nullable=False)
    summary            : Mapped[Optional[str]]     = mapped_column(Text, nullable=True)
    invalidation       : Mapped[Optional[str]]     = mapped_column(Text, nullable=True)
    raw_response       : Mapped[Optional[str]]     = mapped_column(Text, nullable=True)
    feedback           : Mapped[Optional[str]]     = mapped_column(String(20), nullable=True)
    feedback_note      : Mapped[Optional[str]]     = mapped_column(Text, nullable=True)
    is_active          : Mapped[bool]              = mapped_column(Boolean, nullable=False, server_default=text("TRUE"))
    expires_at         : Mapped[Optional[datetime]]= mapped_column(DateTime, nullable=True)
    created_at         : Mapped[datetime]          = mapped_column(DateTime, nullable=False, server_default=text("NOW()"))

    post         : Mapped[Post]             = relationship("Post", back_populates="analyses")
    trades       : Mapped[list[Trade]]      = relationship("Trade", back_populates="analysis")
    price_alerts : Mapped[list[PriceAlert]] = relationship("PriceAlert", back_populates="analysis", cascade="all, delete-orphan")


class PriceAlert(Base):
    __tablename__ = "price_alerts"
    __table_args__ = (
        CheckConstraint(
            "alert_type IN ('ENTRY_1','ENTRY_2','ENTRY_3','ENTRY_4','ABSOLUTE_STOP','STOP_LOSS','TAKE_PROFIT','TAKE_PROFIT_2','SHORT_ENTRY')",
            name="price_alerts_type_check",
        ),
        CheckConstraint("status IN ('PENDING','PENDING_SLOT','TRIGGERED','CANCELLED')", name="price_alerts_status_check"),
        Index("idx_price_alerts_analysis_id", "analysis_id"),
        Index("idx_price_alerts_coin_symbol", "coin_symbol"),
    )

    id           : Mapped[int]               = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    analysis_id  : Mapped[int]               = mapped_column(BigInteger, ForeignKey("analyses.id", ondelete="CASCADE"), nullable=False)
    coin_symbol  : Mapped[str]               = mapped_column(String(20), nullable=False)
    target_price : Mapped[Decimal]           = mapped_column(Numeric(18, 2), nullable=False)
    alert_type   : Mapped[str]               = mapped_column(String(20), nullable=False)
    status       : Mapped[str]               = mapped_column(String(20), nullable=False, server_default=text("'PENDING'"))
    triggered_at : Mapped[Optional[datetime]]= mapped_column(DateTime, nullable=True)
    created_at   : Mapped[datetime]          = mapped_column(DateTime, nullable=False, server_default=text("NOW()"))

    analysis: Mapped[Analysis] = relationship("Analysis", back_populates="price_alerts")


class PostLink(Base):
    __tablename__ = "post_links"
    __table_args__ = (
        CheckConstraint("link_type IN ('tradingview', 'youtube', 'other')", name="post_links_type_check"),
        Index("idx_post_links_post_id", "post_id"),
        Index("idx_post_links_link_type", "link_type"),
    )

    id        : Mapped[int]      = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    post_id   : Mapped[int]      = mapped_column(BigInteger, ForeignKey("posts.id", ondelete="CASCADE"), nullable=False)
    url       : Mapped[str]      = mapped_column(Text, nullable=False)
    link_type : Mapped[str]      = mapped_column(String(20), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=text("NOW()"))

    post: Mapped[Post] = relationship("Post", back_populates="links")


class VideoMemo(Base):
    __tablename__ = "video_memos"
    __table_args__ = (
        Index("idx_video_memos_post_id", "post_id"),
        Index("idx_video_memos_created_at", "created_at"),
    )

    id        : Mapped[int]          = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    post_id   : Mapped[Optional[int]]= mapped_column(BigInteger, ForeignKey("posts.id", ondelete="SET NULL"), nullable=True)
    content   : Mapped[str]          = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime]     = mapped_column(DateTime, nullable=False, server_default=text("NOW()"))

    post: Mapped[Optional[Post]] = relationship("Post", back_populates="video_memos")


class NewsArticle(Base):
    __tablename__ = "news_articles"
    __table_args__ = (
        CheckConstraint("sentiment IN ('BULLISH', 'BEARISH', 'NEUTRAL')", name="news_sentiment_check"),
        CheckConstraint("impact_level IN ('HIGH', 'MEDIUM', 'LOW')", name="news_impact_check"),
        Index("idx_news_published_at", "published_at"),
        Index("idx_news_source", "source"),
        Index("idx_news_impact", "impact_level"),
    )

    id            : Mapped[int]               = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source        : Mapped[str]               = mapped_column(String(50), nullable=False)
    external_id   : Mapped[Optional[str]]     = mapped_column(String(255), nullable=True, unique=True)
    title         : Mapped[str]               = mapped_column(Text, nullable=False)
    summary       : Mapped[Optional[str]]     = mapped_column(Text, nullable=True)
    url           : Mapped[str]               = mapped_column(Text, nullable=False)
    published_at  : Mapped[datetime]          = mapped_column(DateTime, nullable=False)
    sentiment     : Mapped[Optional[str]]     = mapped_column(String(10), nullable=True)
    impact_level  : Mapped[Optional[str]]     = mapped_column(String(10), nullable=True)
    related_coins : Mapped[Any]               = mapped_column(JSONB, nullable=False, server_default=text("'[]'"))
    gpt_analysis  : Mapped[Optional[str]]     = mapped_column(Text, nullable=True)
    is_processed  : Mapped[bool]              = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    collected_at  : Mapped[datetime]          = mapped_column(DateTime, nullable=False, server_default=text("NOW()"))


class UserProfile(Base):
    __tablename__ = "user_profiles"
    __table_args__ = (
        CheckConstraint("risk_tolerance IN ('CONSERVATIVE', 'MODERATE', 'AGGRESSIVE')", name="user_risk_check"),
        CheckConstraint("trading_mode IN ('AUTO', 'SEMI_AUTO', 'MANUAL', 'NOTIFY_ONLY')", name="user_mode_check"),
        CheckConstraint("leverage BETWEEN 1 AND 50", name="user_leverage_check"),
        CheckConstraint("auto_ratio BETWEEN 0 AND 100", name="user_auto_ratio_check"),
        Index("idx_user_profiles_telegram_id", "telegram_user_id"),
    )

    id                   : Mapped[int]          = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    telegram_user_id     : Mapped[int]          = mapped_column(BigInteger, nullable=False, unique=True)
    telegram_username    : Mapped[Optional[str]]= mapped_column(String(100), nullable=True)
    risk_tolerance       : Mapped[str]          = mapped_column(String(20), nullable=False, server_default=text("'MODERATE'"))
    total_asset_krw      : Mapped[Optional[int]]= mapped_column(BigInteger, nullable=True)
    leverage             : Mapped[int]          = mapped_column(Integer, nullable=False, server_default=text("1"))
    leverage_config      : Mapped[Any]          = mapped_column(JSONB, nullable=False, server_default=text("'{}'"))
    trading_mode         : Mapped[str]          = mapped_column(String(20), nullable=False, server_default=text("'SEMI_AUTO'"))
    auto_ratio           : Mapped[int]          = mapped_column(Integer, nullable=False, server_default=text("50"))
    preferred_coins      : Mapped[Any]          = mapped_column(JSONB, nullable=False, server_default=text("'[]'"))
    onboarding_completed : Mapped[bool]         = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    created_at           : Mapped[datetime]     = mapped_column(DateTime, nullable=False, server_default=text("NOW()"))
    updated_at           : Mapped[datetime]     = mapped_column(DateTime, nullable=False, server_default=text("NOW()"))


class Trade(Base):
    __tablename__ = "trades"
    __table_args__ = (
        CheckConstraint("side IN ('BUY', 'SELL')", name="trades_side_check"),
        CheckConstraint("status IN ('PENDING', 'FILLED', 'FAILED', 'CANCELLED')", name="trades_status_check"),
        CheckConstraint("mode IN ('AUTO', 'SEMI_AUTO', 'MANUAL')", name="trades_mode_check"),
        Index("idx_trades_analysis_id", "analysis_id"),
        Index("idx_trades_status", "status"),
        Index("idx_trades_executed_at", "executed_at"),
    )

    id             : Mapped[int]               = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    analysis_id    : Mapped[int]               = mapped_column(BigInteger, ForeignKey("analyses.id"), nullable=False)
    position_id    : Mapped[Optional[int]]     = mapped_column(BigInteger, ForeignKey("positions.id"), nullable=True)
    symbol         : Mapped[str]               = mapped_column(String(20), nullable=False)
    side           : Mapped[str]               = mapped_column(String(10), nullable=False)
    qty            : Mapped[Decimal]           = mapped_column(Numeric(18, 8), nullable=False)
    price          : Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    status         : Mapped[str]               = mapped_column(String(20), nullable=False, server_default=text("'PENDING'"))
    bybit_order_id : Mapped[Optional[str]]     = mapped_column(String(100), nullable=True)
    stop_loss_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    mode           : Mapped[str]               = mapped_column(String(20), nullable=False)
    executed_at    : Mapped[Optional[datetime]]= mapped_column(DateTime, nullable=True)

    analysis : Mapped[Analysis]           = relationship("Analysis", back_populates="trades")
    position : Mapped[Optional[Position]] = relationship("Position", back_populates="trades")


class Position(Base):
    __tablename__ = "positions"
    __table_args__ = (
        CheckConstraint("side IN ('LONG', 'SHORT')", name="positions_side_check"),
        CheckConstraint("status IN ('OPEN', 'CLOSED')", name="positions_status_check"),
        Index("idx_positions_analysis_id", "analysis_id"),
        Index("idx_positions_coin_symbol", "coin_symbol"),
        Index("idx_positions_status", "status"),
    )

    id                    : Mapped[int]               = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    analysis_id           : Mapped[int]               = mapped_column(BigInteger, ForeignKey("analyses.id"), nullable=False)
    coin_symbol           : Mapped[str]               = mapped_column(String(20), nullable=False)
    side                  : Mapped[str]               = mapped_column(String(10), nullable=False)
    avg_entry_price       : Mapped[Decimal]           = mapped_column(Numeric(18, 2), nullable=False)
    initial_capital_usdt  : Mapped[Decimal]           = mapped_column(Numeric(18, 2), nullable=False)
    leverage              : Mapped[int]               = mapped_column(SmallInteger, nullable=False)
    current_qty           : Mapped[Decimal]           = mapped_column(Numeric(18, 8), nullable=False)
    current_stop_loss     : Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    current_take_profit_1 : Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    current_take_profit_2 : Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    tp1_executed          : Mapped[bool]              = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    tp2_executed          : Mapped[bool]              = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    add_buy_count         : Mapped[int]               = mapped_column(SmallInteger, nullable=False, server_default=text("0"))
    bybit_position_idx    : Mapped[Optional[int]]     = mapped_column(SmallInteger, nullable=True)
    status                : Mapped[str]               = mapped_column(String(10), nullable=False, server_default=text("'OPEN'"))
    opened_at             : Mapped[datetime]          = mapped_column(DateTime, nullable=False, server_default=text("NOW()"))
    closed_at             : Mapped[Optional[datetime]]= mapped_column(DateTime, nullable=True)

    trades: Mapped[list[Trade]] = relationship("Trade", back_populates="position")


class MarketContext(Base):
    __tablename__ = "market_context"
    __table_args__ = (
        Index("idx_market_context_post_id", "post_id"),
        Index("idx_market_context_created_at", "created_at"),
    )

    id         : Mapped[int]           = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    post_id    : Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("posts.id", ondelete="CASCADE"), nullable=True)
    indicator  : Mapped[str]           = mapped_column(String(20), nullable=False)
    state      : Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    key_level  : Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    implication: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    summary    : Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at : Mapped[datetime]      = mapped_column(DateTime, nullable=False, server_default=text("NOW()"))


class EconomicCalendar(Base):
    __tablename__ = "economic_calendars"
    __table_args__ = (
        CheckConstraint("importance IN ('HIGH', 'MEDIUM', 'LOW')", name="econ_cal_importance_check"),
        Index("idx_econ_cal_event_date", "event_date"),
        Index("idx_econ_cal_importance", "importance"),
    )

    id          : Mapped[int]               = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source      : Mapped[str]               = mapped_column(String(50), nullable=False, server_default=text("'finnhub'"))
    external_id : Mapped[Optional[str]]     = mapped_column(String(255), nullable=True, unique=True)
    event_name  : Mapped[str]               = mapped_column(Text, nullable=False)
    event_date  : Mapped[date]              = mapped_column(Date, nullable=False)
    event_time  : Mapped[Optional[Any]]     = mapped_column(Time, nullable=True)
    importance  : Mapped[str]               = mapped_column(String(10), nullable=False)
    description : Mapped[Optional[str]]     = mapped_column(Text, nullable=True)
    created_at  : Mapped[datetime]          = mapped_column(DateTime, nullable=False, server_default=text("NOW()"))


class Settings(Base):
    __tablename__ = "settings"
    __table_args__ = (
        CheckConstraint("mode IN ('AUTO', 'SEMI_AUTO', 'MANUAL')", name="settings_mode_check"),
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
