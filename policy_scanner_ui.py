import streamlit as st
from sqlalchemy import create_engine, inspect, text
import pandas as pd
from datetime import datetime
import random
import string
import logging
import json
import hashlib
import os
import html

from config import config as load_config

# Set page configuration
st.set_page_config(
    page_title="Policy Scanner",
    page_icon="🛡️",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .main {
        padding: 2rem;
    }
    .stButton button {
        width: 100%;
        background-color: #4CAF50;
        color: white;
        padding: 0.5rem;
        border-radius: 5px;
    }
    .comparison-card {
        background-color: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 1rem;
    }
    .header-style {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 5px;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database Configuration
def build_database_url():
    env_url = os.getenv("DATABASE_URL")
    if env_url:
        return env_url

    cfg = load_config()
    host = cfg.get("host", "localhost")
    user = cfg.get("user", "")
    password = cfg.get("password", "")
    database = cfg.get("database", "")
    port = cfg.get("port", 5432)

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


DATABASE_URL = build_database_url()
engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)


def ensure_user_tables():
    statements = [
        """
        CREATE TABLE IF NOT EXISTS user_info (
            id SERIAL PRIMARY KEY,
            user_id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            contact TEXT NOT NULL,
            email TEXT NOT NULL,
            dob DATE,
            gender TEXT,
            nicotine_status TEXT,
            status TEXT NOT NULL DEFAULT 'Active',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_user_info_email_contact ON user_info (email, contact)",
        """
        CREATE TABLE IF NOT EXISTS user_coverage_selected (
            id SERIAL PRIMARY KEY,
            user_info_id INTEGER NOT NULL REFERENCES user_info(id) ON DELETE CASCADE,
            coverage_selected INTEGER NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
    ]

    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


try:
    ensure_user_tables()
    inspector = inspect(engine)
    if not inspector.has_table("user_info") or not inspector.has_table("user_coverage_selected"):
        raise RuntimeError("Required user tables are missing after initialization.")
except Exception as e:
    logger.exception("Failed to ensure user tables")
    st.error(
        "Database setup error: required tables ('user_info', 'user_coverage_selected') could not be created."
    )
    st.write("Run the SQL in `user_creation_commands.sql` using a PostgreSQL admin user, then rerun the app.")
    st.code("psql -U postgres -d prd_policyscanner -f user_creation_commands.sql")
    st.write(f"Details: {e}")
    st.stop()

def generate_user_hash(email, contact):
    """Generate a unique hash for the user"""
    return hashlib.md5(f"{email}{contact}".encode()).hexdigest()

def check_existing_user(email, contact):
    """Check if user already exists in database"""
    query = """
    SELECT 
        ui.*, 
        ucs.coverage_selected,
        ucs.created_at as coverage_date
    FROM user_info ui
    LEFT JOIN user_coverage_selected ucs ON ui.id = ucs.user_info_id
    WHERE ui.email = :email AND ui.contact = :contact
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), {"email": email, "contact": contact})
            user_data = result.fetchone()
            return user_data._asdict() if user_data else None
    except Exception as e:
        logger.error(f"Error checking existing user: {e}")
        return None

def run_query(query, params=None):
    try:
        if isinstance(params, dict):
            params = {k: _normalize_sql_value(v) for k, v in params.items()}
        with engine.connect() as connection:
            result = connection.execute(text(query), params)
            if result.returns_rows:
                return pd.DataFrame(result.fetchall(), columns=result.keys())
            return None
    except Exception as e:
        logger.error(f"Database query error: {str(e)}")
        raise e


def _normalize_sql_value(value):
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return value.item()
    except Exception:
        pass

    try:
        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime()
    except Exception:
        pass

    return value


def run_insert(query, params):
    try:
        if isinstance(params, dict):
            params = {k: _normalize_sql_value(v) for k, v in params.items()}
        with engine.begin() as connection:
            result = connection.execute(text(query), params)
            return result
    except Exception as e:
        logger.error(f"Database insert error: {str(e)}")
        raise e


def calculate_age(dob, today=None):
    if dob is None:
        return None

    today = today or datetime.now().date()
    years = today.year - dob.year
    if (today.month, today.day) < (dob.month, dob.day):
        years -= 1
    return years

def generate_user_id(name, email):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=4))
    return f"{name[:3]}_{email.split('@')[0]}_{timestamp}_{random_string}".lower()

def format_currency(amount):
    try:
        if amount is None or pd.isna(amount):
            return "N/A"
        return f"${float(amount):,.2f}"
    except Exception:
        return str(amount)


def is_blank(value):
    try:
        if value is None or pd.isna(value):
            return True
    except Exception:
        if value is None:
            return True

    return isinstance(value, str) and not value.strip()


def build_policy_query(filters):
    inspector = inspect(engine)
    if not inspector.has_table("prd_main"):
        raise RuntimeError("Table 'prd_main' not found in the configured PostgreSQL database.")

    prd_main_cols = {col["name"] for col in inspector.get_columns("prd_main")}

    select_cols = ["p.company_name", "p.product_name", "p.annual"]

    if "term_type" in prd_main_cols:
        select_cols.append("p.term_type")

    join_sql = ""
    if inspector.has_table("riders_benefit"):
        rider_cols = {col["name"] for col in inspector.get_columns("riders_benefit")}
        if "product_family" in prd_main_cols and "product_name" in rider_cols:
            join_sql = (
                "\nLEFT JOIN riders_benefit r\n"
                "  ON p.company_name = r.company_name\n"
                " AND p.product_family = r.product_name"
            )
            if "free_riders" in rider_cols:
                select_cols.append("r.free_riders")
            if "paid_riders" in rider_cols:
                select_cols.append("r.paid_riders")

    query = "SELECT\n    " + ",\n    ".join(select_cols) + "\nFROM prd_main p" + join_sql + "\n"

    conditions = []
    params = {}

    face_amount = filters.get("face_amount")
    if face_amount is not None:
        conditions.append("p.face_amount = :face_amount")
        params["face_amount"] = int(face_amount)

    gender = filters.get("gender")
    if gender in {"Male", "Female"}:
        conditions.append("LOWER(TRIM(CAST(p.gender AS TEXT))) = LOWER(:gender)")
        params["gender"] = gender

    policy_type = (filters.get("policy_type") or "").strip()
    if policy_type:
        conditions.append("TRIM(CAST(p.policy_type AS TEXT)) = :policy_type")
        params["policy_type"] = policy_type

    nicotine_status = (filters.get("nicotine_status") or "").strip()
    if nicotine_status:
        conditions.append("LOWER(TRIM(CAST(p.nicotine_status AS TEXT))) = LOWER(:nicotine_status)")
        params["nicotine_status"] = nicotine_status

    age = filters.get("age")
    if age is not None:
        conditions.append("p.age = :age")
        params["age"] = int(age)

    term_type = filters.get("term_type")
    if term_type and "term_type" in prd_main_cols:
        conditions.append("CAST(p.term_type AS TEXT) = :term_type")
        params["term_type"] = str(int(term_type))

    if conditions:
        query += "WHERE\n    " + "\n    AND ".join(conditions) + "\n"

    query += "ORDER BY p.annual ASC"
    return query, params


def get_policy_type_options():
    try:
        inspector = inspect(engine)
        if not inspector.has_table("prd_main"):
            return ["CI"]

        prd_main_cols = {col["name"] for col in inspector.get_columns("prd_main")}
        if "policy_type" not in prd_main_cols:
            return ["CI"]

        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT DISTINCT policy_type
                    FROM prd_main
                    WHERE policy_type IS NOT NULL
                      AND TRIM(CAST(policy_type AS TEXT)) <> ''
                    ORDER BY policy_type ASC
                    """
                )
            ).fetchall()

        options = [str(row[0]).strip() for row in rows if row and row[0] is not None]
        options = [o for o in options if o]
        return options or ["CI"]
    except Exception as e:
        logger.warning(f"Failed to load policy_type options: {e}")
        return ["CI"]


def get_face_amount_options(policy_type=None):
    try:
        inspector = inspect(engine)
        if not inspector.has_table("prd_main"):
            return []

        prd_main_cols = {col["name"] for col in inspector.get_columns("prd_main")}
        if "face_amount" not in prd_main_cols:
            return []

        params = {}
        query = """
            SELECT DISTINCT face_amount
            FROM prd_main
            WHERE face_amount IS NOT NULL
        """

        if policy_type and "policy_type" in prd_main_cols:
            query += "\n  AND TRIM(CAST(policy_type AS TEXT)) = :policy_type"
            params["policy_type"] = str(policy_type).strip()

        query += "\nORDER BY face_amount ASC"

        with engine.connect() as conn:
            rows = conn.execute(text(query), params).fetchall()

        options = []
        for row in rows:
            if not row:
                continue
            val = _normalize_sql_value(row[0])
            if val is None:
                continue
            try:
                options.append(int(val))
            except Exception:
                continue

        return sorted(set(options))
    except Exception as e:
        logger.warning(f"Failed to load face_amount options: {e}")
        return []


def get_term_type_options(policy_type=None, face_amount=None):
    try:
        inspector = inspect(engine)
        if not inspector.has_table("prd_main"):
            return []

        prd_main_cols = {col["name"] for col in inspector.get_columns("prd_main")}
        if "term_type" not in prd_main_cols:
            return []

        params = {}
        query = """
            SELECT DISTINCT TRIM(CAST(term_type AS TEXT)) AS term_type
            FROM prd_main
            WHERE term_type IS NOT NULL
              AND TRIM(CAST(term_type AS TEXT)) <> ''
        """

        if policy_type and "policy_type" in prd_main_cols:
            query += "\n  AND TRIM(CAST(policy_type AS TEXT)) = :policy_type"
            params["policy_type"] = str(policy_type).strip()

        if face_amount is not None and "face_amount" in prd_main_cols:
            query += "\n  AND face_amount = :face_amount"
            params["face_amount"] = int(face_amount)

        with engine.connect() as conn:
            rows = conn.execute(text(query), params).fetchall()

        options = []
        for row in rows:
            if not row:
                continue
            val = (row[0] or "").strip()
            if not val:
                continue
            try:
                options.append(int(float(val)))
            except Exception:
                continue

        return sorted(set(options))
    except Exception as e:
        logger.warning(f"Failed to load term_type options: {e}")
        return []


def get_age_bounds():
    try:
        inspector = inspect(engine)
        if not inspector.has_table("prd_main"):
            return 18, 75

        with engine.connect() as conn:
            row = conn.execute(text("SELECT MIN(age), MAX(age) FROM prd_main WHERE age IS NOT NULL")).fetchone()

        if row and row[0] is not None and row[1] is not None:
            return int(_normalize_sql_value(row[0])), int(_normalize_sql_value(row[1]))

    except Exception as e:
        logger.warning(f"Failed to load age bounds: {e}")

    return 18, 75


def display_comparison_data(comparison_data):
    st.markdown("### Available Insurance Plans")

    if comparison_data is None or comparison_data.empty:
        st.info("No plans to display.")
        return

    max_cards = 60
    if len(comparison_data) > max_cards:
        st.info(f"Showing first {max_cards} plans (out of {len(comparison_data)}). Refine filters to narrow down.")
        comparison_data = comparison_data.head(max_cards)

    def parse_riders(value):
        if is_blank(value):
            return []
        parts = [p.strip() for p in str(value).split("|")]
        return [p for p in parts if p]

    cols = st.columns(3)
    for idx, row in comparison_data.iterrows():
        company_name = html.escape(str(row.get("company_name", "")).strip())
        product_name = html.escape(str(row.get("product_name", "")).strip())
        annual = row.get("annual")

        term_type = row.get("term_type") if "term_type" in comparison_data.columns else None
        free_riders = row.get("free_riders") if "free_riders" in comparison_data.columns else None
        paid_riders = row.get("paid_riders") if "paid_riders" in comparison_data.columns else None

        with cols[idx % 3]:
            with st.container():
                st.markdown(
                    f"""
                    <div class="comparison-card">
                        <h3 style="color: #1f77b4;">{company_name}</h3>
                        <h4>{product_name}</h4>
                        <div class="metric-card">
                            <p style="color: #666;">Annual Premium</p>
                            <h2 style="color: #2ecc71;">{html.escape(format_currency(annual))}</h2>
                        </div>
                        <div style="margin: 0.75rem 0;">
                            {"<p><strong>Term Type:</strong> " + html.escape(str(term_type)) + "</p>" if not is_blank(term_type) else ""}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                free_list = parse_riders(free_riders)
                paid_list = parse_riders(paid_riders)
                if free_list or paid_list:
                    with st.expander("Riders", expanded=False):
                        if free_list:
                            st.markdown("**Free Riders**")
                            st.markdown("\n".join([f"- {html.escape(x)}" for x in free_list]))
                        if paid_list:
                            st.markdown("**Paid Riders**")
                            st.markdown("\n".join([f"- {html.escape(x)}" for x in paid_list]))

# Initialize session state
if 'user_data' not in st.session_state:
    st.session_state.user_data = None

# App Header
st.title("🛡️ Policy Scanner - Insurance Comparison")

# Main content area
if st.session_state.user_data is None:
    # User Input Form
    with st.form("user_input_form"):
        st.markdown('<div class="header-style">', unsafe_allow_html=True)
        st.header("Enter Your Details")
        st.markdown('</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            name = st.text_input("Full Name", max_chars=50)
            dob = st.date_input("Date of Birth", min_value=datetime(1900, 1, 1))
            dob_age = calculate_age(dob)
            min_age, max_age = get_age_bounds()
            default_quote_age = (
                dob_age
                if dob_age is not None and min_age <= dob_age <= max_age
                else (35 if min_age <= 35 <= max_age else min_age)
            )
            quote_age = st.number_input(
                "Age (Years)",
                min_value=min_age,
                max_value=max_age,
                value=int(default_quote_age),
                step=1,
            )
            if dob_age is not None:
                st.caption(f"DOB-based age: {dob_age}. Using age {quote_age} for comparison.")
            else:
                st.caption(f"Using age {quote_age} for comparison.")
            contact = st.text_input("Contact Number", max_chars=15)
            email = st.text_input("Email Address")
        
        with col2:
            gender = st.selectbox("Gender", ["Male", "Female", "Other"])
            smoker_status = st.radio("Smoking Status", ["Non-Smoker", "Smoker"])
            policy_type_options = get_policy_type_options()
            default_policy_idx = policy_type_options.index("CI") if "CI" in policy_type_options else 0
            policy_type = st.selectbox("Policy Type", policy_type_options, index=default_policy_idx)
            coverage_amount = st.number_input("Coverage Amount ($)", min_value=0, step=1000)
            term_type_years = st.number_input("Term Type (Years) (0 = Any)", min_value=0, step=1)
        
        submitted = st.form_submit_button("Find Insurance Plans")
        
        if submitted:
            try:
                # Check for existing user
                existing_user = check_existing_user(email, contact)
                nicotine_status = "smoker" if smoker_status == "Smoker" else "non smoker"
                policy_type = (policy_type or "").strip() or None

                if not all([name, email, contact, coverage_amount > 0]):
                    st.error("Please fill in all required fields.")
                    st.stop()

                user_age = int(quote_age)

                coverage_amount = int(coverage_amount)
                term_type_years = int(term_type_years)

                face_amount_options = get_face_amount_options(policy_type)
                if face_amount_options and coverage_amount not in face_amount_options:
                    st.error(
                        f"No data for coverage {coverage_amount} with policy type '{policy_type}'. "
                        f"Available coverages: {', '.join(map(str, face_amount_options))}."
                    )
                    st.stop()

                term_type_options = get_term_type_options(policy_type, coverage_amount)
                if term_type_years > 0 and term_type_options and term_type_years not in term_type_options:
                    st.error(
                        f"No data for term type {term_type_years} with coverage {coverage_amount} and policy type '{policy_type}'. "
                        f"Available term types: {', '.join(map(str, term_type_options))}."
                    )
                    st.stop()
                
                if existing_user:
                    st.session_state.user_data = existing_user
                    st.success("Welcome back! We've loaded your previous information.")
                else:
                    # Input validation
                    # Generate user ID and insert new user
                    user_id = generate_user_id(name, email)
                    
                    insert_user_query = """
                    INSERT INTO user_info 
                    (user_id, name, contact, email, dob, gender, nicotine_status, status, created_at, updated_at)
                    VALUES 
                    (:user_id, :name, :contact, :email, :dob, :gender, :nicotine_status, 'Active', NOW(), NOW())
                    """
                    
                    user_params = {
                        "user_id": user_id,
                        "name": name,
                        "contact": contact,
                        "email": email,
                        "dob": dob,
                        "gender": gender,
                        "nicotine_status": nicotine_status,
                    }

                    run_insert(insert_user_query, user_params)
                    
                    # Get the user_info_id and insert coverage
                    user_result = run_query("SELECT id FROM user_info WHERE user_id = :user_id", {"user_id": user_id})
                    
                    if user_result is not None and not user_result.empty:
                        user_info_id = user_result.iloc[0]['id']
                        
                        coverage_query = """
                        INSERT INTO user_coverage_selected 
                        (user_info_id, coverage_selected)
                        VALUES 
                        (:user_info_id, :coverage_amount)
                        """
                        
                        run_insert(coverage_query, {
                            "user_info_id": user_info_id,
                            "coverage_amount": coverage_amount
                        })
                        
                        # Store user data in session state
                        st.session_state.user_data = {
                            "user_id": user_id,
                            "name": name,
                            "email": email,
                            "contact": contact,
                            "dob": dob,
                            "coverage_selected": coverage_amount
                        }
                        
                        st.success("Profile created successfully!")
                
                # Store additional calculation data
                st.session_state.coverage_amount = coverage_amount
                st.session_state.user_age = user_age
                st.session_state.term_type_years = term_type_years
                st.session_state.gender = gender
                st.session_state.nicotine_status = nicotine_status
                st.session_state.policy_type = policy_type
                
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                logger.error(f"Error in form submission: {str(e)}")

else:
    # Display user profile and allow edit
    st.sidebar.markdown("### Your Profile")
    st.sidebar.write(f"Name: {st.session_state.user_data['name']}")
    st.sidebar.write(f"Email: {st.session_state.user_data['email']}")
    if st.sidebar.button("Edit Profile"):
        st.session_state.user_data = None
        st.experimental_rerun()

# Display insurance comparison if user data exists
if st.session_state.get('user_data') is not None:
    try:
        term_type_filter = st.session_state.get("term_type_years")
        term_type_filter = term_type_filter if term_type_filter and term_type_filter > 0 else None

        comparison_query, query_params = build_policy_query(
            {
                "face_amount": st.session_state.get("coverage_amount"),
                "age": st.session_state.get("user_age"),
                "gender": st.session_state.get("gender"),
                "nicotine_status": st.session_state.get("nicotine_status"),
                "policy_type": st.session_state.get("policy_type") or "CI",
                "term_type": term_type_filter,
            }
        )

        comparison_data = run_query(comparison_query, query_params)

        if comparison_data is not None and not comparison_data.empty:
            display_comparison_data(comparison_data)
        else:
            st.warning("No policies match your selected filters.")
    
    except Exception as e:
        st.error(f"Error retrieving comparison data: {str(e)}")
        logger.error(f"Comparison query error: {str(e)}")

# Add footer
st.markdown("""
<div style="text-align: center; margin-top: 2rem; padding: 1rem; background-color: #f0f2f6; border-radius: 5px;">
    <p>© 2024 Policy Scanner. All rights reserved.</p>
</div>
""", unsafe_allow_html=True)
