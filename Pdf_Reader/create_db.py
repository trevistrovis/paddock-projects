from server import app, db
from sqlalchemy import text

def init_db():
    with app.app_context():
        try:
            # Create the keyword_groups table first
            db.session.execute(text("""
                CREATE TABLE IF NOT EXISTS keyword_groups (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(255) NOT NULL,
                    keywords TEXT NOT NULL
                )
            """))
            
            # Check if group_id column exists
            result = db.session.execute(text("""
                SELECT COUNT(*) as count 
                FROM information_schema.columns 
                WHERE table_name = 'searches' 
                AND column_name = 'group_id'
            """))
            column_exists = result.fetchone()[0] > 0
            
            # Add group_id column if it doesn't exist
            if not column_exists:
                db.session.execute(text("""
                    ALTER TABLE searches
                    ADD COLUMN group_id INT,
                    ADD CONSTRAINT fk_group
                    FOREIGN KEY (group_id) REFERENCES keyword_groups(id)
                """))
            
            db.session.commit()
            print("Database updated successfully!")
            
        except Exception as e:
            print(f"Error updating database: {e}")
            db.session.rollback()

if __name__ == "__main__":
    init_db()