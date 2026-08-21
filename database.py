import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path


DATABASE_PATH = Path(__file__).with_name('mawkib.db')
_WRITE_LOCK = threading.Lock()
_CUSTOMER_FIELDS = {
    'event_type',
    'budget_level',
    'style_preference',
    'city',
    'guest_count',
    'venue',
    'event_date',
    'converted',
    'preferred_contact_method',
    'customer_intent',
    'last_action',
    'shown_actions',
}


def _now():
    return datetime.now().astimezone().isoformat(timespec='seconds')


def get_connection():
    connection = sqlite3.connect(DATABASE_PATH, timeout=15)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA foreign_keys = ON')
    connection.execute('PRAGMA journal_mode = WAL')
    return connection


def init_db():
    with _WRITE_LOCK, get_connection() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
                message TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_conversations_session_time
            ON conversations (session_id, timestamp, id);

            CREATE TABLE IF NOT EXISTS customers (
                session_id TEXT PRIMARY KEY,
                event_type TEXT,
                budget_level TEXT,
                style_preference TEXT,
                city TEXT,
                guest_count INTEGER,
                venue TEXT,
                event_date TEXT,
                converted INTEGER NOT NULL DEFAULT 0 CHECK (converted IN (0, 1)),
                preferred_contact_method TEXT,
                customer_intent TEXT,
                last_action TEXT,
                shown_actions TEXT NOT NULL DEFAULT '[]',
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_customers_last_seen
            ON customers (last_seen DESC);

            CREATE TABLE IF NOT EXISTS analytics (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_conversations INTEGER NOT NULL DEFAULT 0,
                converted_leads INTEGER NOT NULL DEFAULT 0,
                most_requested_event TEXT,
                common_objections TEXT NOT NULL DEFAULT '[]',
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS engagement_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_page TEXT,
                customer_stage TEXT,
                FOREIGN KEY (session_id) REFERENCES customers(session_id)
            );

            CREATE INDEX IF NOT EXISTS idx_engagement_session_time
            ON engagement_events (session_id, timestamp DESC, id DESC);

            CREATE INDEX IF NOT EXISTS idx_engagement_type_time
            ON engagement_events (event_type, timestamp DESC);
            """
        )
        customer_columns = {
            row['name']
            for row in connection.execute('PRAGMA table_info(customers)').fetchall()
        }
        for column_name in (
            'preferred_contact_method', 'customer_intent', 'last_action',
            'shown_actions', 'venue', 'event_date',
        ):
            if column_name not in customer_columns:
                default_clause = " DEFAULT '[]'" if column_name == 'shown_actions' else ''
                connection.execute(f'ALTER TABLE customers ADD COLUMN {column_name} TEXT{default_clause}')
        connection.execute(
            """
            INSERT OR IGNORE INTO analytics (
                id, total_conversations, converted_leads,
                most_requested_event, common_objections, updated_at
            ) VALUES (1, 0, 0, NULL, '[]', ?)
            """,
            (_now(),),
        )


def ensure_customer(session_id):
    timestamp = _now()
    with _WRITE_LOCK, get_connection() as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO customers (session_id, first_seen, last_seen)
            VALUES (?, ?, ?)
            """,
            (session_id, timestamp, timestamp),
        )
        connection.execute(
            'UPDATE customers SET last_seen = ? WHERE session_id = ?',
            (timestamp, session_id),
        )


def save_message(session_id, role, message):
    if role not in {'user', 'assistant'}:
        raise ValueError('role must be user or assistant')

    timestamp = _now()
    with _WRITE_LOCK, get_connection() as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO customers (session_id, first_seen, last_seen)
            VALUES (?, ?, ?)
            """,
            (session_id, timestamp, timestamp),
        )
        connection.execute(
            """
            INSERT INTO conversations (session_id, timestamp, role, message)
            VALUES (?, ?, ?, ?)
            """,
            (session_id, timestamp, role, message),
        )
        connection.execute(
            'UPDATE customers SET last_seen = ? WHERE session_id = ?',
            (timestamp, session_id),
        )
        _refresh_analytics(connection)


def get_history(session_id, limit=20):
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT role, message, timestamp
            FROM (
                SELECT id, role, message, timestamp
                FROM conversations
                WHERE session_id = ?
                ORDER BY id DESC
                LIMIT ?
            )
            ORDER BY id ASC
            """,
            (session_id, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def update_customer(session_id, **fields):
    updates = {key: value for key, value in fields.items() if key in _CUSTOMER_FIELDS and value is not None}
    if not updates:
        ensure_customer(session_id)
        return

    timestamp = _now()
    if 'converted' in updates:
        updates['converted'] = int(bool(updates['converted']))

    assignments = ', '.join(f'{field} = ?' for field in updates)
    values = list(updates.values())

    with _WRITE_LOCK, get_connection() as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO customers (session_id, first_seen, last_seen)
            VALUES (?, ?, ?)
            """,
            (session_id, timestamp, timestamp),
        )
        connection.execute(
            f'UPDATE customers SET {assignments}, last_seen = ? WHERE session_id = ?',
            (*values, timestamp, session_id),
        )
        _refresh_analytics(connection)


def mark_converted(session_id):
    update_customer(session_id, converted=True)


def record_event(session_id, event_type, event_page=None, customer_stage=None):
    timestamp = _now()
    ensure_customer(session_id)
    with _WRITE_LOCK, get_connection() as connection:
        connection.execute(
            """
            INSERT INTO engagement_events (
                session_id, timestamp, event_type, event_page, customer_stage
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                session_id,
                timestamp,
                str(event_type or '').strip(),
                str(event_page or '').strip()[:300] or None,
                str(customer_stage or '').strip()[:80] or None,
            ),
        )
        connection.execute(
            'UPDATE customers SET last_action = ?, last_seen = ? WHERE session_id = ?',
            (str(event_type or '').strip(), timestamp, session_id),
        )


def get_recent_events(session_id, limit=50):
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT timestamp, event_type, event_page, customer_stage
            FROM engagement_events
            WHERE session_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (session_id, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def get_customer(session_id):
    with get_connection() as connection:
        row = connection.execute(
            'SELECT * FROM customers WHERE session_id = ?',
            (session_id,),
        ).fetchone()
    return dict(row) if row else None


def get_dashboard_data():
    with get_connection() as connection:
        _refresh_analytics(connection)
        analytics_row = connection.execute(
            'SELECT * FROM analytics WHERE id = 1'
        ).fetchone()
        event_rows = connection.execute(
            """
            SELECT event_type AS label, COUNT(*) AS value
            FROM customers
            WHERE event_type IS NOT NULL AND TRIM(event_type) != ''
            GROUP BY event_type
            ORDER BY value DESC, label ASC
            """
        ).fetchall()
        recent_rows = connection.execute(
            """
            SELECT session_id, last_seen, event_type, style_preference,
                   budget_level, city, guest_count, converted,
                   preferred_contact_method, customer_intent, last_action, shown_actions,
                   venue, event_date
            FROM customers
            ORDER BY last_seen DESC
            LIMIT 20
            """
        ).fetchall()
        social_rows = connection.execute(
            """
            SELECT event_type AS label, COUNT(*) AS value
            FROM engagement_events
            GROUP BY event_type
            ORDER BY value DESC, label ASC
            """
        ).fetchall()
        average_length = connection.execute(
            """
            SELECT COALESCE(AVG(message_count), 0) AS average_length
            FROM (
                SELECT COUNT(*) AS message_count
                FROM conversations
                GROUP BY session_id
            )
            """
        ).fetchone()['average_length']

    total = analytics_row['total_conversations'] if analytics_row else 0
    converted = analytics_row['converted_leads'] if analytics_row else 0
    return {
        'summary': {
            'total_conversations': total,
            'converted_leads': converted,
            'conversion_rate': round((converted / total * 100) if total else 0, 1),
            'average_conversation_length': round(float(average_length or 0), 1),
            'most_requested_event': analytics_row['most_requested_event'] if analytics_row else None,
            'common_objections': json.loads(analytics_row['common_objections'] or '[]') if analytics_row else [],
            'total_social_clicks': sum(row['value'] for row in social_rows),
        },
        'events': [dict(row) for row in event_rows],
        'social_engagement': [dict(row) for row in social_rows],
        'recent_conversations': [dict(row) for row in recent_rows],
    }


def get_conversation(session_id):
    customer = get_customer(session_id)
    if not customer:
        return None
    return {
        'customer': customer,
        'messages': get_history(session_id, limit=500),
        'engagement_events': get_recent_events(session_id, limit=100),
    }


def _refresh_analytics(connection):
    total = connection.execute(
        'SELECT COUNT(*) AS count FROM customers'
    ).fetchone()['count']
    converted = connection.execute(
        'SELECT COUNT(*) AS count FROM customers WHERE converted = 1'
    ).fetchone()['count']
    event_row = connection.execute(
        """
        SELECT event_type, COUNT(*) AS count
        FROM customers
        WHERE event_type IS NOT NULL AND TRIM(event_type) != ''
        GROUP BY event_type
        ORDER BY count DESC, event_type ASC
        LIMIT 1
        """
    ).fetchone()

    objection_terms = {
        'السعر': ['غالي', 'السعر', 'الاسعار', 'الأسعار'],
        'الميزانية': ['ميزانية', 'الميزانيه', 'محدودة', 'محدوده'],
        'التردد': ['بفكر', 'سأفكر', 'متردد', 'مو متأكد'],
        'الوقت': ['مستعجل', 'الوقت ضيق', 'قريب جدًا', 'قريب جدا'],
    }
    objection_counts = []
    user_messages = connection.execute(
        "SELECT message FROM conversations WHERE role = 'user'"
    ).fetchall()
    for label, terms in objection_terms.items():
        count = sum(
            1
            for row in user_messages
            if any(term in row['message'].lower() for term in terms)
        )
        if count:
            objection_counts.append({'label': label, 'count': count})
    objection_counts.sort(key=lambda item: (-item['count'], item['label']))

    connection.execute(
        """
        UPDATE analytics
        SET total_conversations = ?, converted_leads = ?,
            most_requested_event = ?, common_objections = ?, updated_at = ?
        WHERE id = 1
        """,
        (
            total,
            converted,
            event_row['event_type'] if event_row else None,
            json.dumps(objection_counts, ensure_ascii=False),
            _now(),
        ),
    )


init_db()
