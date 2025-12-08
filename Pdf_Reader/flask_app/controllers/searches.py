# from flask_app import app
# from flask import render_template, redirect, send_from_directory, request
# from pdfminer.high_level import extract_text
# import mysql.connector
# import re
# import os

# def extract_text_from_pdf_file(file_path):

#     text = extract_text(file_path)
#     return text.split("\x0c")
#     # text_pages = []
#     # with fitz.open(file_path) as pdf:
#     #     for page_num in range(pdf.page_count):
#     #         page = pdf[page_num]
#     #         text_pages.append(page.get_text())
#     # return text_pages

# def find_sentence_in_pdf(text, keywords):
#     sentences = re.split(r'(?<=[.!?])\s', text)
#     results = []
#     for sentence in sentences:
#         if keywords.lower() in sentence.lower():
#             results.append(sentence.strip())
#     return results


# def search_keywords_in_text(text, keywords):
#     results = []
#     for keyword in keywords:
#         sentences_with_keyword = find_sentence_in_pdf(text, keyword)
#         for sentence in sentences_with_keyword:
#             results.append((keyword, sentence))
#         return results

# def search_keywords_in_pdf(file_path, keywords):
#     pages_text = extract_text_from_pdf_file(file_path)
#     all_results = {}
#     for page_num, page_text in enumerate(pages_text, start = 1):
#         page_results = search_keywords_in_text(page_text, keywords)
#         if page_results:
#             all_results[f"Page {page_num}"] = page_results
#     return all_results

# def save_search_to_database(file_name, keyword, page_number, snippet):
#     """
#     Saves search results to the MySQL database.
#     """
#     try:
#         connection = mysql.connector.connect(
#             host='localhost',
#             database='pdf_search',
#             user='root',
#             password='root'
#         )
#         if connection.is_connected():
#             cursor = connection.cursor()
#             query = """
#                 INSERT INTO searches (file_name, keyword, page_number, snippet)
#                 VALUES (%s, %s, %s, %s)
#             """
#             cursor.execute(query, (file_name, keyword, page_number, snippet))
#             connection.commit()
#     except mysql.connector.Error as e:
#         print("Error while connecting to MySQL", e)
#     finally:
#         if connection.is_connected():
#             cursor.close()
#             connection.close()

# @app.route('/', methods = ["GET", "POST"])
# def home():
#     results = {}
#     file_name = None
#     if request.method == "POST":
#         keywords = request.form["keywords"].split(",")
#         pdf_file = request.files["pdf_file"]
#         if pdf_file:
#             original_filename = os.path.basename(pdf_file.filename)
#             safe_filename = original_filename.replace("","")
#             pdf_path = os.path.join("uploads", safe_filename)
#             pdf_file.save(pdf_path)
#             file_name = safe_filename
#             results = search_keywords_in_pdf(pdf_path, keywords)
#     return render_template("index.html", results=results, file_name=file_name)

# @app.route ('/save', methods = ["POST"])
# def save():
#     file_name = request.form.get("file_name")
#     keyword = request.form.getlist("keyword")
#     page_number = request.form.getlist("page_number")
#     snippet = request.form.getlist("snippet")

#     for i in range(len(keyword)):
#         save_search_to_database(file_name, keyword[i], int(page_number[i]), snippet[i])
#     return redirect('/history')

# @app.route ('/history')
# def history():
#     connection = mysql.connector.connect(
#         host = 'localhost',
#         database = 'pdf_search',
#         user = 'root',
#         password = 'root'
#     )
#     cursor = connection.cursor(dictionary=True)
#     cursor.execute("SELECT * FROM searches ORDER BY search_date DESC")
#     results = cursor.fetchall()
#     cursor.close()
#     connection.close()
#     return render_template("history.html", results=results)

# @app.route('/uploads/<filename>')
# def uploaded_file(filename):
#     return send_from_directory('uploads',filename)

# @app.route('/delete/<int:id>', methods=["POST"])
# def delete_save(id):
#     try:
#         connection = mysql.connector.connect(
#             host='localhost',
#             database='pdf_search',
#             user='root',
#             password='root'
#         )
#         if connection.is_connected():
#             cursor = connection.cursor()
#             query = "DELETE FROM searches WHERE id = %s"
#             cursor.execute(query, (id,))
#             connection.commit()
#     except mysql.connector.Error as e:
#         print("Error while connecting to MySQL", e)
#     finally:
#         if connection.is_connected():
#             cursor.close()
#             connection.close()
#     return redirect('/history')