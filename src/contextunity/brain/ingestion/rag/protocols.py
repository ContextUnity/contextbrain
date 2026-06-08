"""Protocol surfaces for optional third-party ingestion dependencies."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from email.message import Message
from pathlib import Path
from typing import Protocol, Self, TypeGuard

from contextunity.core.narrowing import as_int, as_str, object_attr, tuple_item_at, tuple_len
from contextunity.core.types import JsonDict, is_object_list


class _PdfPage(Protocol):
    def get_text(self, kind: str) -> list[tuple[object, ...]] | list[object]: ...


class _PdfDocument(Protocol):
    page_count: int

    def get_toc(self) -> list[tuple[int, str, int]]: ...

    def load_page(self, page_index: int) -> _PdfPage: ...

    def close(self) -> None: ...

    def __iter__(self) -> Iterator[_PdfPage]: ...


class _FitzOpenFactory(Protocol):
    def __call__(self, path: str | Path) -> _PdfDocument: ...


class _FitzModule(Protocol):
    open: _FitzOpenFactory


def _is_fitz_module(value: object) -> TypeGuard[_FitzModule]:
    open_factory = object_attr(value, "open")
    return callable(open_factory)


def _is_pdf_document(value: object) -> TypeGuard[_PdfDocument]:
    page_count = object_attr(value, "page_count")
    load_page = object_attr(value, "load_page")
    get_toc = object_attr(value, "get_toc")
    close = object_attr(value, "close")
    return (
        isinstance(page_count, int)
        and callable(load_page)
        and callable(get_toc)
        and callable(close)
    )


def _load_fitz_module() -> _FitzModule | None:
    for name in ("fitz", "pymupdf"):
        try:
            mod_obj: object = importlib.import_module(name)
        except ImportError:
            continue
        if _is_fitz_module(mod_obj):
            return mod_obj
    return None


def fitz_is_available() -> bool:
    """Return whether PyMuPDF can be loaded."""
    return _load_fitz_module() is not None


def fitz_open(path: str | Path) -> _PdfDocument:
    """Open a PDF via PyMuPDF without propagating ``Any`` from dynamic imports."""
    mod = _load_fitz_module()
    if mod is None:
        raise RuntimeError("PyMuPDF (fitz/pymupdf) is not installed")
    open_factory = object_attr(mod, "open")
    if not callable(open_factory):
        raise RuntimeError("PyMuPDF open() is not callable")
    doc_obj: object = open_factory(str(path))
    if not _is_pdf_document(doc_obj):
        raise RuntimeError("PyMuPDF document returned an invalid surface")
    return doc_obj


def fitz_get_toc(path: str | Path) -> list[tuple[int, str, int]]:
    """Read PDF table of contents as (level, title, page) tuples."""
    doc = fitz_open(path)
    try:
        toc_obj: object = doc.get_toc()
        if not is_object_list(toc_obj):
            return []
        out: list[tuple[int, str, int]] = []
        for entry_obj in toc_obj:
            if tuple_len(entry_obj) < 3:
                continue
            level = as_int(tuple_item_at(entry_obj, 0), default=0)
            title = as_str(tuple_item_at(entry_obj, 1))
            page = as_int(tuple_item_at(entry_obj, 2), default=1)
            out.append((level, title, page))
        return out
    finally:
        _ = doc.close()


def fitz_page_count(path: str | Path) -> int:
    """Return total page count for a PDF."""
    doc = fitz_open(path)
    try:
        return doc.page_count
    finally:
        _ = doc.close()


def fitz_page_text(doc: _PdfDocument, page_index: int) -> str:
    """Extract plain text from a single PDF page."""
    page = doc.load_page(page_index)
    get_text = object_attr(page, "get_text")
    if not callable(get_text):
        return ""
    text_obj: object = get_text()
    return text_obj if isinstance(text_obj, str) else ""


def pymupdf4llm_to_markdown(path: str | Path, **kwargs: object) -> str:
    """Convert PDF to markdown via pymupdf4llm when installed."""
    mod = import_module_object("pymupdf4llm")
    to_markdown = object_attr(mod, "to_markdown")
    if not callable(to_markdown):
        raise RuntimeError("pymupdf4llm.to_markdown is not callable")
    content_obj: object = to_markdown(str(path), **kwargs)
    return content_obj if isinstance(content_obj, str) else ""


def try_activate_pymupdf_layout() -> None:
    """Activate pymupdf.layout when present (optional quality improvement)."""
    try:
        layout_mod: object = importlib.import_module("pymupdf.layout")
    except ImportError:
        return
    activate = object_attr(layout_mod, "activate")
    if callable(activate):
        _ = activate()


def toc_entry_to_json(level: int, title: str, page: int) -> JsonDict:
    """Normalize a TOC tuple into ingestion JSON."""
    return {
        "level": level,
        "title": title,
        "page": page,
        "start_page": page,
    }


class _HtmlAnchorTag(Protocol):
    def get(self, key: str, default: object = ...) -> object: ...


class _HtmlTitle(Protocol):
    string: object | None


class _HtmlSoup(Protocol):
    title: _HtmlTitle | None

    def find(self, name: str, *, attrs: dict[str, str] | None = ...) -> _HtmlMetaTag | None: ...

    def find_all(self, name: str) -> list[_HtmlAnchorTag]: ...

    def get_text(self, separator: str, *, strip: bool = ...) -> str: ...


class _HtmlMetaTag(Protocol):
    def get(self, key: str, default: object = ...) -> object: ...


class _HttpResponse(Protocol):
    headers: Message

    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: object,
        exc_val: object,
        exc_tb: object,
    ) -> None: ...

    def geturl(self) -> str: ...

    def read(self) -> bytes: ...


def _is_html_soup(value: object) -> TypeGuard[_HtmlSoup]:
    find_all = getattr(value, "find_all", None)
    return callable(find_all)


def _is_http_response(value: object) -> TypeGuard[_HttpResponse]:
    geturl = getattr(value, "geturl", None)
    read = getattr(value, "read", None)
    return callable(geturl) and callable(read)


def import_module_object(name: str) -> object:
    """Load a module without propagating ``importlib``'s ``Any``."""
    return importlib.import_module(name)


def bs4_parse(html: str) -> _HtmlSoup:
    """Parse HTML with BeautifulSoup when ``bs4`` is installed."""
    bs4 = import_module_object("bs4")
    factory = object_attr(bs4, "BeautifulSoup")
    if not callable(factory):
        raise RuntimeError("bs4.BeautifulSoup is not callable")
    parsed: object = factory(html, "html.parser")
    if not _is_html_soup(parsed):
        raise RuntimeError("bs4.BeautifulSoup returned an invalid document")
    return parsed


def soup_anchor_hrefs(soup: _HtmlSoup) -> list[str]:
    """Extract absolute-ready href strings from anchor tags."""
    find_all = object_attr(soup, "find_all")
    if not callable(find_all):
        return []
    anchors_obj: object = find_all("a")
    if not is_object_list(anchors_obj):
        return []
    links: list[str] = []
    for anchor in anchors_obj:
        get_method = object_attr(anchor, "get")
        if not callable(get_method):
            continue
        href = get_method("href")
        if isinstance(href, str) and href.strip():
            links.append(href)
    return links


def soup_decompose_tags(soup: _HtmlSoup, names: list[str]) -> None:
    """Remove script/style/noscript tags from a parsed document."""
    find_all = object_attr(soup, "find_all")
    if not callable(find_all):
        return
    for name in names:
        tags_obj: object = find_all(name)
        if not is_object_list(tags_obj):
            continue
        for tag in tags_obj:
            decompose = object_attr(tag, "decompose")
            if callable(decompose):
                _ = decompose()


def soup_title_text(soup: _HtmlSoup) -> str:
    title_obj = object_attr(soup, "title")
    if title_obj is None:
        return ""
    string_val = object_attr(title_obj, "string")
    if isinstance(string_val, str):
        return string_val.strip()
    return ""


def soup_meta_content(soup: _HtmlSoup, *, name: str | None = None, prop: str | None = None) -> str:
    find = object_attr(soup, "find")
    if not callable(find):
        return ""
    attrs: dict[str, str] = {}
    if name is not None:
        attrs["name"] = name
    if prop is not None:
        attrs["property"] = prop
    meta_obj: object = find("meta", attrs=attrs)
    if meta_obj is None:
        return ""
    get_method = object_attr(meta_obj, "get")
    if not callable(get_method):
        return ""
    content = get_method("content")
    return content.strip() if isinstance(content, str) else ""


def soup_plain_text(soup: _HtmlSoup) -> str:
    get_text = object_attr(soup, "get_text")
    if not callable(get_text):
        return ""
    text_obj: object = get_text(" ", strip=True)
    return text_obj if isinstance(text_obj, str) else ""


def trafilatura_extract(html: str) -> str:
    """Extract main text with trafilatura when installed."""
    mod = import_module_object("trafilatura")
    extract = object_attr(mod, "extract")
    if not callable(extract):
        return ""
    content_obj: object = extract(html, include_comments=False, include_tables=False)
    return content_obj if isinstance(content_obj, str) else ""


def urlopen_response(req: object, *, timeout: float) -> _HttpResponse:
    """``urllib.request.urlopen`` with a typed response surface."""
    urllib_request = import_module_object("urllib.request")
    urlopen = object_attr(urllib_request, "urlopen")
    if not callable(urlopen):
        raise RuntimeError("urllib.request.urlopen is not callable")
    resp_obj: object = urlopen(req, timeout=timeout)
    if not _is_http_response(resp_obj):
        raise RuntimeError("urllib.request.urlopen returned an invalid response")
    return resp_obj


class _GcsBlob(Protocol):
    def upload_from_filename(self, filename: str, *, content_type: str = ...) -> object: ...


class _GcsBucket(Protocol):
    def blob(self, blob_name: str) -> _GcsBlob: ...


class _GcsClient(Protocol):
    def bucket(self, bucket_name: str) -> _GcsBucket: ...


class _DiscoveryOperationName(Protocol):
    @property
    def name(self) -> str: ...


class _DiscoveryLongRunningOperation(Protocol):
    @property
    def operation(self) -> _DiscoveryOperationName: ...

    def result(self, *, timeout: float) -> object: ...


class _DiscoveryDocumentClient(Protocol):
    def branch_path(
        self,
        *,
        project: str,
        location: str,
        data_store: str,
        branch: str,
    ) -> str: ...

    def import_documents(self, *, request: object) -> _DiscoveryLongRunningOperation: ...


def _is_discovery_document_client(value: object) -> TypeGuard[_DiscoveryDocumentClient]:
    branch_path = object_attr(value, "branch_path")
    import_documents = object_attr(value, "import_documents")
    return callable(branch_path) and callable(import_documents)


def _discovery_document_client_factory(value: object) -> Callable[..., _DiscoveryDocumentClient]:
    if not callable(value):
        raise RuntimeError("DocumentServiceClient factory is not callable")

    def factory(*args: object, **kwargs: object) -> _DiscoveryDocumentClient:
        client_obj: object = value(*args, **kwargs)
        if not _is_discovery_document_client(client_obj):
            raise RuntimeError("DocumentServiceClient returned an invalid client")
        return client_obj

    return factory


@dataclass(frozen=True, slots=True)
class DiscoveryEngineV1Bindings:
    """Typed accessors for ``google.cloud.discoveryengine_v1``."""

    document_service_client: Callable[..., _DiscoveryDocumentClient]
    import_documents_request: Callable[..., object]
    gcs_source: Callable[..., object]
    reconciliation_incremental: object


def _is_gcs_client(value: object) -> TypeGuard[_GcsClient]:
    bucket = object_attr(value, "bucket")
    return callable(bucket)


def gcs_storage_client() -> _GcsClient:
    """Construct a GCS client without propagating library ``Any``."""
    mod = import_module_object("google.cloud.storage")
    factory = object_attr(mod, "Client")
    if not callable(factory):
        raise RuntimeError("google.cloud.storage.Client is not callable")
    client_obj: object = factory()
    if not _is_gcs_client(client_obj):
        raise RuntimeError("google.cloud.storage.Client returned an invalid client")
    return client_obj


def discovery_engine_v1_bindings() -> DiscoveryEngineV1Bindings:
    """Load Discovery Engine v1 symbols without propagating library ``Any``."""
    mod = import_module_object("google.cloud.discoveryengine_v1")
    document_service_client = object_attr(mod, "DocumentServiceClient")
    import_documents_request = object_attr(mod, "ImportDocumentsRequest")
    gcs_source = object_attr(mod, "GcsSource")
    request_class = object_attr(mod, "ImportDocumentsRequest")
    reconciliation_mode = object_attr(request_class, "ReconciliationMode")
    reconciliation_incremental = object_attr(reconciliation_mode, "INCREMENTAL")
    if not callable(document_service_client):
        raise RuntimeError("discoveryengine_v1.DocumentServiceClient is not callable")
    if not callable(import_documents_request):
        raise RuntimeError("discoveryengine_v1.ImportDocumentsRequest is not callable")
    if not callable(gcs_source):
        raise RuntimeError("discoveryengine_v1.GcsSource is not callable")
    return DiscoveryEngineV1Bindings(
        document_service_client=_discovery_document_client_factory(document_service_client),
        import_documents_request=import_documents_request,
        gcs_source=gcs_source,
        reconciliation_incremental=reconciliation_incremental,
    )


__all__ = [
    "PdfDocument",
    "PdfPage",
    "DiscoveryEngineV1Bindings",
    "bs4_parse",
    "discovery_engine_v1_bindings",
    "fitz_get_toc",
    "gcs_storage_client",
    "fitz_is_available",
    "fitz_open",
    "fitz_page_count",
    "fitz_page_text",
    "import_module_object",
    "pymupdf4llm_to_markdown",
    "soup_anchor_hrefs",
    "soup_decompose_tags",
    "soup_meta_content",
    "soup_plain_text",
    "soup_title_text",
    "toc_entry_to_json",
    "trafilatura_extract",
    "try_activate_pymupdf_layout",
    "urlopen_response",
]

PdfPage = _PdfPage
PdfDocument = _PdfDocument
