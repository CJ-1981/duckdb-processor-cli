import pytest
import os
import pandas as pd
from gradio_app import (
    add_report_section, 
    remove_report_section, 
    clear_report_sections, 
    generate_report_markdown, 
    generate_report_pdf, 
    apply_report_template,
    REPORT_TEMPLATES,
    load_data
)
import gradio_app

@pytest.fixture
def mock_processor(tmp_path):
    # Create a small dummy CSV
    csv_file = tmp_path / "test.csv"
    df = pd.DataFrame({"category": ["A", "B"], "amount": [10, 20]})
    df.to_csv(csv_file, index=False)
    
    # Load it into the app's global state
    gradio_app.load_data(str(csv_file), True, False)
    return gradio_app.global_processor

def test_section_management():
    sections = []
    # Test adding
    sections, st = add_report_section(sections, "Text/Note", "Heading 1", "Body text")
    assert len(sections) == 1
    assert sections[0]["heading"] == "Heading 1"
    
    # Test removing
    sections, st = remove_report_section(sections, 0)
    assert len(sections) == 0
    
    # Test clearing
    sections, st = add_report_section(sections, "Data Summary", "Summary", "")
    sections, st = add_report_section(sections, "Schema Info", "Schema", "")
    sections, st = clear_report_sections()
    assert sections == []

def test_template_application():
    # Test applying a valid template
    sections, st, v, p = apply_report_template("Basic Summary")
    assert len(sections) == 3
    assert sections[1]["type"] == "Schema Info"
    assert "✅ Applied template" in st

def test_markdown_generation(mock_processor):
    sections = [
        {"type": "Text/Note", "heading": "Intro", "body": "My notes"},
        {"type": "Data Summary", "heading": "Stats", "body": ""}
    ]
    md = generate_report_markdown("Test Report", "Tester", sections)
    
    assert "# Test Report" in md
    assert "**Author:** Tester" in md
    assert "## Intro" in md
    assert "My notes" in md
    assert "## Stats" in md
    # Data summary should be rendered since mock_processor is active
    assert "- **Total Rows:** 2" in md

def test_pdf_generation(mock_processor, tmp_path):
    sections = [
        {"type": "Text/Note", "heading": "PDF Intro", "body": "PDF notes"},
        {"type": "SQL Results Table", "heading": "SQL Results", "body": ""}
    ]
    
    # Mock some last results
    mock_processor.last_result = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    
    pdf_path = generate_report_pdf("PDF Test", "Tester", sections)
    
    # Verify file exists and is not empty
    assert os.path.exists(pdf_path)
    assert os.path.getsize(pdf_path) > 0
    
    # Clean up
    if os.path.exists(pdf_path):
        os.remove(pdf_path)

def test_report_export_dispatcher(mock_processor):
    from gradio_app import export_report_file
    sections = [{"type": "Text/Note", "heading": "Export Test", "body": "test"}]
    
    # Test Markdown export type
    md_path = export_report_file("md", "Export Title", "Exporter", sections)
    assert md_path.endswith(".md")
    assert os.path.exists(md_path)
    os.remove(md_path)
    
    # Test PDF export type
    pdf_path = export_report_file("pdf", "Export Title", "Exporter", sections)
    assert pdf_path.endswith(".pdf")
    assert os.path.exists(pdf_path)
    os.remove(pdf_path)
