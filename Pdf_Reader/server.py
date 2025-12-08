from flask import Flask, render_template, request, redirect, send_from_directory, url_for, flash, jsonify
from werkzeug.utils import secure_filename
from pdfminer.high_level import extract_text
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_login import LoginManager, UserMixin, login_user, logout_user, current_user, login_required
import fitz # PyMuPDF
import re
import os
import sys
import webbrowser
from pathlib import Path
import pymysql
from sqlalchemy import text

pymysql.install_as_MySQLdb()

def get_base_path():
    if getattr(sys, 'frozen', False):
        # Running as compiled executable
        return Path(sys._MEIPASS)
    else:
        # Running as script
        return Path(__file__).parent

def get_upload_folder():
    base_path = get_base_path()
    upload_path = base_path / 'flask_app' / 'uploads'
    # Create the uploads directory if it doesn't exist
    upload_path.mkdir(parents=True, exist_ok=True)
    return str(upload_path)

def get_template_folder():
    base_path = get_base_path()
    template_path = base_path / 'flask_app' / 'templates'
    if template_path.exists():
        return str(template_path)
    return 'templates'  # fallback to default

def get_static_folder():
    base_path = get_base_path()
    static_path = base_path / 'flask_app' / 'static'
    if static_path.exists():
        return str(static_path)
    return 'static'  # fallback to default

def get_db_path():
    if getattr(sys, 'frozen', False):
        # When running as exe, store database in same directory as executable
        db_dir = os.path.dirname(sys.executable)
        db_path = os.path.join(db_dir, 'pdf_search.db')
        # Ensure the directory exists
        os.makedirs(db_dir, exist_ok=True)
        return db_path
    else:
        # When running as script, store database in project directory
        return os.path.join(os.path.dirname(__file__), 'pdf_search.db')

app = Flask(__name__, 
           template_folder=get_template_folder(),
           static_folder=get_static_folder())
app.secret_key = 'Paddock'

# Set up upload folder
UPLOAD_FOLDER = get_upload_folder()
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# MySQL Database Configuration
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:@127.0.0.1/pdf_search'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize extensions
db = SQLAlchemy(app)
bcrypt = Bcrypt(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# Create tables for all models
def init_db():
    with app.app_context():
        try:
            # Create tables
            db.create_all()
            print("Database tables created successfully")
        except Exception as e:
            print(f"Error creating database tables: {e}")
            # Try to create database in current working directory as fallback
            fallback_path = os.path.join(os.getcwd(), 'pdf_search.db')
            print(f"Attempting to create database at fallback location: {fallback_path}")
            app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{fallback_path}'
            try:
                db.create_all()
                print("Database created successfully at fallback location:", fallback_path)
            except Exception as e2:
                print(f"Error creating database at fallback location: {e2}")
                raise

# Initialize the database
init_db()

class User(db.Model, UserMixin):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(255), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)
    email = db.Column(db.String(255), unique=True, nullable=False)

class Search(db.Model):
    __tablename__ = 'searches'
    id = db.Column(db.Integer, primary_key=True)
    file_name = db.Column(db.String(255), nullable=False)
    keyword = db.Column(db.String(255), nullable=False)
    page_number = db.Column(db.Integer, nullable=False)
    snippet = db.Column(db.Text, nullable=False)
    group_id = db.Column(db.Integer, db.ForeignKey('keyword_groups.id'), nullable=True)

class KeywordGroup(db.Model):
    __tablename__ = 'keyword_groups'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    keywords = db.Column(db.Text, nullable=False)

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

def extract_text_from_pdf_file(file_path):

    # text = extract_text(file_path)
    # return text.split("\x0c")
    text_pages = []
    with fitz.open(file_path) as pdf:
        for page_num in range(pdf.page_count):
            page = pdf[page_num]
            text_pages.append(page.get_text())
    return text_pages
    
def find_sentence_in_pdf(text, keywords):
        # Compile regex for efficiency
        keyword_pattern = re.compile(re.escape(keywords.lower()), re.IGNORECASE)
        sentences = re.split(r'(?<=[.!?])\s', text)
        return [sentence.strip() for sentence in sentences if keyword_pattern.search(sentence)]
# def find_sentence_in_pdf(text, keywords):
#     sentences = re.split(r'(?<=[.!?])\s', text)
#     results = []
#     for sentence in sentences:
#         if keywords.lower() in sentence.lower():
#             results.append(sentence.strip())
#     return results    
def search_keywords_in_text(text, keywords):
        results = []
        for keyword in keywords:
            sentences_with_keyword = find_sentence_in_pdf(text, keyword)
            results.extend([(keyword, sentence) for sentence in sentences_with_keyword])
        return results
# def search_keywords_in_text(text, keywords):
#     results = []
#     for keyword in keywords:
#         sentences_with_keyword = find_sentence_in_pdf(text, keyword)
#         for sentence in sentences_with_keyword:
#             results.append((keyword, sentence))
#         return results    
def search_keywords_in_pdf(file_path, keywords):
        pages_text = extract_text_from_pdf_file(file_path)
        all_results = {}
        for page_num, page_text in enumerate(pages_text, start=1):
            page_results = search_keywords_in_text(page_text, keywords)
            if page_results:
                all_results[f"Page {page_num}"] = page_results
        return all_results

# def search_keywords_in_pdf(file_path, keywords):
#     pages_text = extract_text_from_pdf_file(file_path)
#     all_results = {}
#     for page_num, page_text in enumerate(pages_text, start = 1):
#         page_results = search_keywords_in_text(page_text, keywords)
#         if page_results:
#             all_results[f"Page {page_num}"] = page_results
#     return all_results

def save_search_to_database(file_name, keyword, page_number, snippet):
    """
    Saves search results to the SQLite database using SQLAlchemy session.
    """
    try:
        new_search = Search(
            file_name=file_name,
            keyword=keyword,
            page_number=page_number,
            snippet=snippet
        )
        db.session.add(new_search)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        print("Error while saving to database:", e)

@app.route('/', methods=["GET", "POST"])
@login_required
def home():
    results = {}
    file_name = None
    groups = KeywordGroup.query.all()
    
    # Get list of uploaded documents
    uploaded_files = []
    if os.path.exists(app.config['UPLOAD_FOLDER']):
        uploaded_files = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if f.endswith('.pdf')]
    
    if request.method == "POST":
        group_id = request.form.get("keyword_group")
        keywords = request.form.get("keywords", "").split(",")
        
        if group_id:
            group = KeywordGroup.query.get(group_id)
            if group:
                keywords.extend(group.keywords.split(","))
        
        # Check if we're using a selected file or a new upload
        selected_file = request.form.get("selected_file")
        if selected_file and keywords:
            # Use the selected file from the uploaded documents
            file_name = selected_file
            pdf_path = os.path.join(app.config['UPLOAD_FOLDER'], selected_file)
            
            # Remove duplicates and empty strings from keywords
            keywords = list(set(filter(None, [k.strip() for k in keywords])))
            results = search_keywords_in_pdf(pdf_path, keywords)
            
            # Save search results with group information
            for page, page_results in results.items():
                for keyword, snippet in page_results:
                    search = Search(
                        file_name=file_name,
                        keyword=keyword,
                        page_number=int(page.split()[1]),
                        snippet=snippet,
                        group_id=group_id if group_id else None
                    )
                    db.session.add(search)
            db.session.commit()
        
        else:
            # Handle new file upload
            pdf_file = request.files.get("pdf_file")
            if pdf_file and keywords:
                original_filename = pdf_file.filename
                safe_filename = secure_filename(original_filename)
                pdf_path = os.path.join(app.config['UPLOAD_FOLDER'], safe_filename)
                pdf_file.save(pdf_path)
                file_name = safe_filename
                
                # Remove duplicates and empty strings
                keywords = list(set(filter(None, [k.strip() for k in keywords])))
                results = search_keywords_in_pdf(pdf_path, keywords)
                
                # Save search results with group information
                for page, page_results in results.items():
                    for keyword, snippet in page_results:
                        search = Search(
                            file_name=file_name,
                            keyword=keyword,
                            page_number=int(page.split()[1]),
                            snippet=snippet,
                            group_id=group_id if group_id else None
                        )
                        db.session.add(search)
                db.session.commit()
    
    return render_template('index.html', 
                         results=results, 
                         file_name=file_name, 
                         groups=groups,
                         username=current_user.username,
                         uploaded_files=uploaded_files)

@app.route('/keyword_groups', methods=['GET'])
@login_required
def keyword_groups():
    groups = KeywordGroup.query.all()
    return render_template('keyword_groups.html', groups=groups)

@app.route('/keyword_groups/add', methods=['POST'])
@login_required
def add_keyword_group():
    name = request.form.get('name')
    keywords = request.form.get('keywords')
    if name and keywords:
        group = KeywordGroup(name=name, keywords=keywords)
        db.session.add(group)
        db.session.commit()
        flash('Keyword group added successfully!', 'success')
    return redirect(url_for('keyword_groups'))

@app.route('/keyword_groups/delete/<int:id>', methods=['POST'])
@login_required
def delete_keyword_group(id):
    group = KeywordGroup.query.get_or_404(id)
    db.session.delete(group)
    db.session.commit()
    flash('Keyword group deleted successfully!', 'success')
    return redirect(url_for('keyword_groups'))

@app.route('/keyword_groups/edit/<int:id>', methods=['GET', 'POST'])
@login_required
def edit_keyword_group(id):
    group = KeywordGroup.query.get_or_404(id)
    
    if request.method == 'POST':
        name = request.form.get('name')
        keywords = request.form.get('keywords')
        if name and keywords:
            group.name = name
            group.keywords = keywords
            db.session.commit()
            flash('Keyword group updated successfully!', 'success')
            return redirect(url_for('keyword_groups'))
    
    return render_template('keyword_groups.html', edit_group=group, groups=KeywordGroup.query.all())

def get_uploaded_documents():
    return os.listdir(UPLOAD_FOLDER)

@app.route ('/save', methods = ["POST"])
def save():
    file_name = request.form.get("file_name")
    keyword = request.form.getlist("keyword")
    page_number = request.form.getlist("page_number")
    snippet = request.form.getlist("snippet")

    for i in range(len(keyword)):
        save_search_to_database(file_name, keyword[i], int(page_number[i]), snippet[i])
    return redirect('/history')

@app.route ('/history')
@login_required
def history():
    # Get all search results ordered by file name and keyword
    all_results = db.session.query(Search).order_by(Search.file_name, Search.keyword).all()
    
    # Group results by project name first, then by keyword
    grouped_results = {}
    for result in all_results:
        project_name = result.file_name
        if project_name not in grouped_results:
            grouped_results[project_name] = {
                'count': 0,
                'keywords': {}
            }
        
        keyword = result.keyword
        if keyword not in grouped_results[project_name]['keywords']:
            grouped_results[project_name]['keywords'][keyword] = []
        
        grouped_results[project_name]['keywords'][keyword].append(result.__dict__)
        grouped_results[project_name]['count'] += 1

    return render_template("history.html", grouped_results=grouped_results)

@app.route('/uploads/<filename>')
def uploaded_file(filename):
    try:
        return send_from_directory(UPLOAD_FOLDER, filename, as_attachment=False)
    except Exception as e:
        print(f"Error serving file: {e}")
        return "File not found", 404

@app.route('/delete/<int:id>', methods=['POST'])
@login_required
def delete_save(id):
    try:
        search = db.session.query(Search).get(id)
        if search:
            db.session.delete(search)
            db.session.commit()
            return jsonify({'success': True}), 200
        return jsonify({'success': False, 'error': 'Record not found'}), 404
        
    except Exception as e:
        db.session.rollback()
        print(f"Error deleting search: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/delete/<filename>', methods=["POST"])
@login_required
def delete_file(filename):
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            flash('File deleted successfully', 'success')
        else:
            flash(f'File not found at path: {file_path}', 'error')
    except Exception as e:
        flash(f'Error deleting file: {str(e)}', 'error')
    return redirect(url_for('home'))

@app.route('/delete_project_searches/<project_name>', methods=['POST'])
@login_required
def delete_project_searches(project_name):
    try:
        # Delete all searches for this project
        searches = db.session.query(Search).filter_by(file_name=project_name).all()
        for search in searches:
            db.session.delete(search)
        db.session.commit()
        return jsonify({'success': True}), 200
    except Exception as e:
        db.session.rollback()
        print(f"Error deleting project searches: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        try:
            username = request.form['username']
            password = request.form['password']
            email = request.form['email']

            if not email.endswith('@paddockindustries.com'):
                flash('Only Paddock Pool Equipment Employees can register.', 'danger')
                return redirect(url_for('register'))

            existing_user = User.query.filter_by(email=email).first()
            if existing_user:
                flash('Email already in use. Please login or register with another email.', 'danger')
                return redirect(url_for('login'))
            
            hashed_password = bcrypt.generate_password_hash(password).decode('utf-8')
            new_user = User(username=username, password=hashed_password, email=email)
            
            try:
                db.session.add(new_user)
                db.session.commit()
                flash('Account created. Please log in.', 'success')
                return redirect(url_for('login'))
            except Exception as e:
                db.session.rollback()
                print(f"Database error during registration: {e}")
                flash('Error creating account. Please try again.', 'danger')
                return redirect(url_for('register'))
                
        except Exception as e:
            print(f"Error in registration route: {e}")
            flash('An error occurred during registration. Please try again.', 'danger')
            return redirect(url_for('register'))
    
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        try:
            email = request.form['email']
            password = request.form['password']

            user = User.query.filter_by(email=email).first()
            if user and bcrypt.check_password_hash(user.password, password):
                login_user(user)
                flash('Logged in!', 'success')
                return redirect(url_for('home'))
            else:
                flash('Invalid login attempt. Please try again or register.', 'danger')
        except Exception as e:
            print(f"Error in login route: {e}")
            flash('An error occurred during login. Please try again.', 'danger')
            
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Logged out successfully.', 'success')
    return redirect(url_for('login'))

if __name__ == "__main__":
    # Open the default web browser
    webbrowser.open('http://localhost:5000')
    # Run the Flask app
    app.run(host="0.0.0.0", port=8000, debug=False)