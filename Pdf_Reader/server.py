from flask import Flask, render_template, request
import fitz



app = Flask(__name__)

def extract_text_from_pdf_file(file_path):
    text_pages = []
    with fitz.open(file_path) as pdf:
        for page_num in range(pdf.page_count):
            page = pdf[page_num]
            text_pages.append(page.get_text())
    return text_pages

def search_keywords_in_text(text, keywords, context = 200):
    results = []
    for keyword in keywords:
        start = 0
        while (index := text.find(keyword, start)) != -1:
            start = index + len(keyword)
            snippet = text[max(index - context, 0): min(index + len(keyword) + context, len(text))]
            results.append((keyword, snippet.strip()))
        return results

def search_keywords_in_pdf(file_path, keywords):
    pages_text = extract_text_from_pdf_file(file_path)
    all_results = {}
    for page_num, page_text in enumerate(pages_text, start = 1):
        page_results = search_keywords_in_text(page_text, keywords)
        if page_results:
            all_results[f"Page {page_num}"] = page_results
    return all_results


@app.route('/', methods = ["GET", "POST"])
def home():
    results = {}
    if request.method == "POST":
        keywords = request.form["keywords"].split(",")
        pdf_file = request.files["pdf_file"]
        if pdf_file:
            pdf_path = "uploaded_document.pdf"
            pdf_file.save(pdf_path)
            results = search_keywords_in_pdf(pdf_path, keywords)
    return render_template("index.html", results=results)

if __name__ == "__main__":
    app.run(debug=True)