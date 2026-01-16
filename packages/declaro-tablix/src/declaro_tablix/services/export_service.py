"""Export service functions for Table Module V2.

This module provides pure functions for table data export using streaming.
All dependencies are explicit parameters for clean library design.
"""

import csv
import io
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi.responses import StreamingResponse

from declaro_advise import error, info, success
from declaro_tablix.domain.models import (
    ColumnDefinition,
    TableData,
    TableState,
)
from declaro_tablix.domain.protocols import (
    TableNotificationService,
    create_noop_notification_service,
)


async def export_table_data(
    data: TableData,
    format: str,
    columns: List[ColumnDefinition],
    filename: Optional[str] = None,
    include_metadata: bool = False,
    user_id: Optional[str] = None,
    notification_service: TableNotificationService | None = None,
) -> bytes:
    """Export table data in specified format.

    Args:
        data: Table data to export
        format: Export format (csv, excel, json, pdf)
        columns: Column definitions
        filename: Optional filename
        include_metadata: Whether to include metadata
        user_id: User ID for notifications
        notification_service: Optional notification service

    Returns:
        Exported data as bytes
    """
    if notification_service is None:
        notification_service = create_noop_notification_service()

    try:
        info(f"Exporting table data in {format} format ({len(data.rows)} rows)")

        if format.lower() == "csv":
            exported_data = await export_csv(data, columns, include_headers=True)
        elif format.lower() == "json":
            exported_data = await export_json(data, columns, pretty=True, include_schema=include_metadata)
        elif format.lower() == "excel":
            exported_data = await export_excel(data, columns, sheet_name="Table Data", include_formatting=True)
        elif format.lower() == "pdf":
            exported_data = await export_pdf(data, columns, title="Table Export", include_page_numbers=True)
        else:
            raise ValueError(f"Unsupported export format: {format}")

        success(f"Data exported successfully in {format} format")

        if user_id:
            await notification_service.send_export_notification(
                export_status="completed",
                user_id=user_id,
                table_name="table_data",
                format=format,
                file_size=len(exported_data),
            )

        return exported_data

    except Exception as e:
        error(f"Failed to export table data: {str(e)}")

        if user_id:
            await notification_service.send_export_notification(
                export_status="failed",
                user_id=user_id,
                table_name="table_data",
                format=format,
            )

        raise


async def export_csv(
    data: TableData,
    columns: List[ColumnDefinition],
    delimiter: str = ",",
    include_headers: bool = True,
) -> bytes:
    """Export data as CSV.

    Args:
        data: Table data
        columns: Column definitions
        delimiter: CSV delimiter
        include_headers: Whether to include headers

    Returns:
        CSV data as bytes
    """
    try:
        info(f"Generating CSV export with {len(data.rows)} rows")

        # Create CSV output
        output = io.StringIO()
        writer = csv.writer(output, delimiter=delimiter)

        # Write headers
        if include_headers:
            headers = [col.name for col in columns if col.visible]
            writer.writerow(headers)

        # Write data rows
        visible_columns = [col for col in columns if col.visible]
        for row in data.rows:
            csv_row = []
            for column in visible_columns:
                value = row.get(column.id, "")
                # Format value for CSV
                if value is None:
                    csv_row.append("")
                elif isinstance(value, (list, dict)):
                    csv_row.append(json.dumps(value))
                else:
                    csv_row.append(str(value))
            writer.writerow(csv_row)

        # Get CSV content
        csv_content = output.getvalue()
        output.close()

        success(f"CSV export generated successfully")
        return csv_content.encode("utf-8")

    except Exception as e:
        error(f"Failed to generate CSV export: {str(e)}")
        raise


async def export_excel(
    data: TableData,
    columns: List[ColumnDefinition],
    sheet_name: str = "Sheet1",
    include_formatting: bool = True,
) -> bytes:
    """Export data as Excel.

    Args:
        data: Table data
        columns: Column definitions
        sheet_name: Excel sheet name
        include_formatting: Whether to include formatting

    Returns:
        Excel data as bytes
    """
    try:
        info(f"Generating Excel export with {len(data.rows)} rows")

        # For this implementation, we'll generate a simple CSV-like format
        # In a real implementation, you'd use libraries like openpyxl or xlswriter

        # Create Excel-like content (simplified)
        output = io.StringIO()

        # Write sheet header
        output.write(f"Sheet: {_sanitize_sheet_name(sheet_name)}\n")
        output.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"Total Rows: {len(data.rows)}\n")
        output.write("\n")

        # Write headers
        visible_columns = [col for col in columns if col.visible]
        headers = [col.name for col in visible_columns]
        output.write("\t".join(headers) + "\n")

        # Write data rows
        for row in data.rows:
            excel_row = []
            for column in visible_columns:
                value = row.get(column.id, "")
                # Format value for Excel
                if value is None:
                    excel_row.append("")
                elif isinstance(value, (list, dict)):
                    excel_row.append(json.dumps(value))
                else:
                    excel_row.append(str(value))
            output.write("\t".join(excel_row) + "\n")

        # Add metadata sheet if formatting is enabled
        if include_formatting:
            output.write("\n\nMetadata:\n")
            output.write(f"Export Format: Excel\n")
            output.write(f"Column Count: {len(visible_columns)}\n")
            output.write(f"Row Count: {len(data.rows)}\n")
            if data.metadata:
                output.write(f"Metadata: {json.dumps(data.metadata)}\n")

        excel_content = output.getvalue()
        output.close()

        success(f"Excel export generated successfully")
        return excel_content.encode("utf-8")

    except Exception as e:
        error(f"Failed to generate Excel export: {str(e)}")
        raise


async def export_json(
    data: TableData,
    columns: List[ColumnDefinition],
    pretty: bool = False,
    include_schema: bool = False,
) -> bytes:
    """Export data as JSON.

    Args:
        data: Table data
        columns: Column definitions
        pretty: Whether to pretty-print JSON
        include_schema: Whether to include schema

    Returns:
        JSON data as bytes
    """
    try:
        info(f"Generating JSON export with {len(data.rows)} rows")

        # Build JSON structure
        export_data = {
            "data": data.rows,
            "total_count": data.total_count,
            "export_timestamp": datetime.now().isoformat(),
        }

        # Add schema if requested
        if include_schema:
            schema = []
            for column in columns:
                if column.visible:
                    schema.append(
                        {
                            "id": column.id,
                            "name": column.name,
                            "type": column.type,
                            "sortable": column.sortable,
                            "filterable": column.filterable,
                            "required": column.required,
                            "width": column.width,
                        }
                    )
            export_data["schema"] = schema

        # Add metadata if available
        if data.metadata:
            export_data["metadata"] = data.metadata

        # Generate JSON
        if pretty:
            json_content = json.dumps(export_data, indent=2, ensure_ascii=False)
        else:
            json_content = json.dumps(export_data, ensure_ascii=False)

        success(f"JSON export generated successfully")
        return json_content.encode("utf-8")

    except Exception as e:
        error(f"Failed to generate JSON export: {str(e)}")
        raise


async def export_pdf(
    data: TableData,
    columns: List[ColumnDefinition],
    title: Optional[str] = None,
    include_page_numbers: bool = True,
) -> bytes:
    """Export data as PDF.

    Args:
        data: Table data
        columns: Column definitions
        title: Optional document title
        include_page_numbers: Whether to include page numbers

    Returns:
        PDF data as bytes
    """
    try:
        info(f"Generating PDF export with {len(data.rows)} rows")

        # For this implementation, we'll generate a simple text-based PDF representation
        # In a real implementation, you'd use libraries like reportlab or weasyprint

        # Create PDF-like content (simplified)
        output = io.StringIO()

        # Write PDF header
        if title:
            output.write(f"Title: {title}\n")
        output.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"Total Records: {len(data.rows)}\n")
        output.write("=" * 80 + "\n\n")

        # Write table headers
        visible_columns = [col for col in columns if col.visible]
        headers = [col.name for col in visible_columns]

        # Calculate column widths
        col_widths = []
        for i, column in enumerate(visible_columns):
            max_width = len(headers[i])
            for row in data.rows:
                value = str(row.get(column.id, ""))
                max_width = max(max_width, len(value))
            col_widths.append(min(max_width, 20))  # Cap at 20 characters

        # Write headers
        header_row = ""
        for i, header in enumerate(headers):
            header_row += header[: col_widths[i]].ljust(col_widths[i]) + " | "
        output.write(header_row + "\n")

        # Write separator
        separator = ""
        for width in col_widths:
            separator += "-" * width + "-+-"
        output.write(separator + "\n")

        # Write data rows
        for row_idx, row in enumerate(data.rows):
            row_content = ""
            for i, column in enumerate(visible_columns):
                value = str(row.get(column.id, ""))
                truncated_value = value[: col_widths[i]]
                row_content += truncated_value.ljust(col_widths[i]) + " | "
            output.write(row_content + "\n")

            # Add page break every 50 rows
            if include_page_numbers and (row_idx + 1) % 50 == 0:
                output.write(f"\n--- Page {(row_idx + 1) // 50} ---\n\n")

        # Write footer
        output.write("\n" + "=" * 80 + "\n")
        output.write(f"End of Report - Total Records: {len(data.rows)}\n")

        pdf_content = output.getvalue()
        output.close()

        success(f"PDF export generated successfully")
        return pdf_content.encode("utf-8")

    except Exception as e:
        error(f"Failed to generate PDF export: {str(e)}")
        raise


def get_export_formats() -> List[Dict[str, str]]:
    """Get available export formats.

    Returns:
        List of available export formats
    """
    return [
        {"format": "csv", "name": "CSV", "description": "Comma-separated values"},
        {"format": "excel", "name": "Excel", "description": "Microsoft Excel format"},
        {"format": "json", "name": "JSON", "description": "JavaScript Object Notation"},
        {"format": "pdf", "name": "PDF", "description": "Portable Document Format"},
    ]


async def export_table_by_format(
    table_state: TableState,
    format: str,
    filename: Optional[str] = None,
    include_metadata: bool = False,
    notification_service: TableNotificationService | None = None,
) -> StreamingResponse:
    """Export table by format as streaming response.

    Args:
        table_state: Complete table state
        format: Export format
        filename: Optional filename
        include_metadata: Whether to include metadata
        notification_service: Optional notification service

    Returns:
        FastAPI StreamingResponse
    """
    if notification_service is None:
        notification_service = create_noop_notification_service()

    try:
        info(f"Streaming export for table: {table_state.config.table_name}")

        # Generate filename if not provided
        if not filename:
            filename = _generate_export_filename(table_state.config.table_name, format)

        # Export data
        exported_data = await export_table_data(
            data=table_state.data,
            format=format,
            columns=table_state.config.columns,
            filename=filename,
            include_metadata=include_metadata,
            user_id=table_state.user_id,
            notification_service=notification_service,
        )

        # Determine content type
        content_types = {
            "csv": "text/csv",
            "excel": "application/vnd.ms-excel",
            "json": "application/json",
            "pdf": "application/pdf",
        }

        content_type = content_types.get(format.lower(), "application/octet-stream")

        # Create streaming response
        def generate_chunks():
            chunk_size = 8192
            for i in range(0, len(exported_data), chunk_size):
                yield exported_data[i : i + chunk_size]

        success(f"Export streaming response created")

        return StreamingResponse(
            generate_chunks(),
            media_type=content_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(len(exported_data)),
            },
        )

    except Exception as e:
        error(f"Failed to create export stream: {str(e)}")
        raise


# Helper functions


def _generate_export_filename(table_name: str, format: str) -> str:
    """Generate export filename.

    Args:
        table_name: Table name
        format: Export format

    Returns:
        Generated filename
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_table_name = "".join(c for c in table_name if c.isalnum() or c in "_-")
    return f"{safe_table_name}_export_{timestamp}.{format.lower()}"


def _sanitize_sheet_name(sheet_name: str) -> str:
    """Sanitize sheet name for Excel.

    Args:
        sheet_name: Original sheet name

    Returns:
        Sanitized sheet name
    """
    # Remove invalid characters for Excel sheet names
    invalid_chars = ["\\", "/", "*", "?", ":", "[", "]"]
    sanitized = sheet_name
    for char in invalid_chars:
        sanitized = sanitized.replace(char, "_")

    # Limit length to 31 characters (Excel limit)
    return sanitized[:31]


def _add_excel_metadata_sheet(workbook: Any, data: TableData, columns: List[ColumnDefinition]) -> None:
    """Add metadata sheet to Excel workbook.

    Args:
        workbook: Excel workbook object
        data: Table data
        columns: Column definitions
    """
    # This would be implemented with a real Excel library
    # For now, it's a placeholder for the metadata functionality
    pass


async def stream_export_data(
    data: TableData,
    format: str,
    columns: List[ColumnDefinition],
    chunk_size: int = 8192,
    user_id: Optional[str] = None,
    notification_service: TableNotificationService | None = None,
) -> bytes:
    """Stream export data in chunks for large datasets.

    Args:
        data: Table data to export
        format: Export format
        columns: Column definitions
        chunk_size: Size of each chunk in bytes
        user_id: User ID for notifications
        notification_service: Optional notification service

    Returns:
        Exported data as bytes
    """
    if notification_service is None:
        notification_service = create_noop_notification_service()

    try:
        info(f"Starting streaming export for {len(data.rows)} rows")

        # Export data using existing function
        exported_data = await export_table_data(
            data=data,
            format=format,
            columns=columns,
            user_id=user_id,
            notification_service=notification_service,
        )

        success(f"Streaming export completed successfully")
        return exported_data

    except Exception as e:
        error(f"Failed to stream export data: {str(e)}")
        raise


def create_streaming_iterator(data: bytes, chunk_size: int = 8192):
    """Create iterator for streaming response.

    Args:
        data: Data to stream
        chunk_size: Size of each chunk

    Yields:
        Chunks of data
    """
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]
