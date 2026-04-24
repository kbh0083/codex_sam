from __future__ import annotations

import base64
import codecs
import hashlib
import io
import logging
import re
import tempfile
import zipfile
import zlib
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from urllib.parse import unquote

try:  # pragma: no cover - optional dependency in some environments
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
except Exception:  # pragma: no cover - browser fallback can still work
    default_backend = None
    padding = None
    Cipher = None
    algorithms = None
    modes = None

try:  # pragma: no cover - optional dependency in some environments
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover - standalone copies may omit HTML extras
    BeautifulSoup = None

logger = logging.getLogger(__name__)

_JS_STRING_LITERAL_RE = re.compile(
    r'"((?:\\.|[^"\\])*)"|\'((?:\\.|[^\'\\])*)\'',
    flags=re.DOTALL,
)
_JS_STRING_CONCAT_RE = re.compile(
    r'(?:"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\')'
    r'(?:\s*\+\s*(?:"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'))+',
    flags=re.DOTALL,
)
_BASE64_CANDIDATE_RE = re.compile(r"^[A-Za-z0-9+/]+={0,2}$")
_URLSAFE_BASE64_CANDIDATE_RE = re.compile(r"^[A-Za-z0-9_-]+={0,2}$")


@dataclass(slots=True)
class HtmlTextBlockProjection:
    lines: list[str]


@dataclass(slots=True)
class HtmlTableCellProjection:
    text: str
    rowspan: int
    colspan: int
    source_row: int
    source_col: int
    is_inherited: bool
    row_kind: str | None = None


@dataclass(slots=True)
class HtmlTableRowProjection:
    cells: list[HtmlTableCellProjection]
    is_header: bool
    row_kind: str | None = None


@dataclass(slots=True)
class HtmlTableBlockProjection:
    rows: list[HtmlTableRowProjection]


def _normalize_base64_chunk(s: str) -> str:
    return re.sub(r"\s+", "", s)


def _openssl_md5_evp_to_key_iv(password: bytes, salt: bytes, key_len: int, iv_len: int) -> tuple[bytes, bytes]:
    d = b""
    prev = b""
    while len(d) < key_len + iv_len:
        prev = hashlib.md5(prev + password + salt).digest()
        d += prev
    return d[:key_len], d[key_len : key_len + iv_len]


def _bytes_looks_like_decrypted_html(data: bytes) -> bool:
    for enc in ("utf-8", "utf-8-sig", "cp949"):
        try:
            s = data.decode(enc)
        except UnicodeDecodeError:
            continue
        sl = s.lstrip("\ufeff")
        lower = sl.lower()
        if any(
            marker in lower
            for marker in (
                "<!doctype html",
                "<html",
                "<head",
                "<body",
                "<table",
                "<tr",
                "<td",
                "<th",
                "<div",
                "<span",
                "<p",
                "<meta",
                "<style",
                "<section",
                "<article",
            )
        ):
            return True
        if len(re.findall(r"</?[a-z][a-z0-9]*(?:\s[^<>]*)?>", lower)) >= 6 and len(sl) >= 80:
            return True
    return False


def _decode_js_string_literal(raw: str) -> str:
    try:
        return bytes(raw, "utf-8").decode("unicode_escape")
    except UnicodeDecodeError:
        return raw


def _iter_js_string_literals(blob: str) -> list[str]:
    values: list[str] = []
    for match in _JS_STRING_LITERAL_RE.finditer(blob):
        raw = match.group(1) if match.group(1) is not None else match.group(2)
        if raw is None:
            continue
        values.append(_decode_js_string_literal(raw))
    return values


def _normalize_base64_candidate(candidate: str) -> str | None:
    text = candidate.strip().strip("\"'`")
    if not text:
        return None
    normalized = _normalize_base64_chunk(text)
    if not normalized:
        return None
    if _URLSAFE_BASE64_CANDIDATE_RE.fullmatch(normalized) and not re.search(r"[+/]", normalized):
        normalized = normalized.replace("-", "+").replace("_", "/")
    missing_padding = (-len(normalized)) % 4
    if missing_padding:
        normalized += "=" * missing_padding
    return normalized


def _looks_like_base64_cipher_candidate(candidate: str) -> bool:
    normalized = _normalize_base64_candidate(candidate)
    if normalized is None:
        return False
    if normalized.startswith("U2FsdGVkX1"):
        return True
    if len(normalized) < 48:
        return False
    return bool(_BASE64_CANDIDATE_RE.fullmatch(normalized))


def _candidate_variants(raw: str) -> list[str]:
    seeds = [raw, unescape(raw)]
    if "%" in raw:
        seeds.append(unquote(raw))
        seeds.append(unquote(unescape(raw)))

    variants: list[str] = []
    seen: set[str] = set()
    for seed in seeds:
        normalized = _normalize_base64_candidate(seed)
        if normalized is None or normalized in seen:
            continue
        seen.add(normalized)
        variants.append(normalized)
    return variants


def _decrypt_openssl_salted_aes_b64(ciphertext_b64: str, password: str) -> bytes | None:
    if Cipher is None or algorithms is None or modes is None or padding is None:
        return None
    try:
        raw = base64.b64decode(_normalize_base64_chunk(ciphertext_b64), validate=False)
    except Exception:
        return None
    if len(raw) < 16 or raw[:8] != b"Salted__":
        return None
    salt = raw[8:16]
    ciphertext = raw[16:]
    if not ciphertext:
        return None

    pwd_utf8 = password.encode("utf-8")
    pwd_variants = [pwd_utf8]
    try:
        pwd_variants.append(password.encode("cp437"))
    except UnicodeEncodeError:
        pass
    try:
        pwd_variants.append(password.encode("latin-1"))
    except UnicodeEncodeError:
        pass

    for pwd in pwd_variants:
        for key_bits in (256, 128):
            key_len = key_bits // 8
            try:
                key, iv = _openssl_md5_evp_to_key_iv(pwd, salt, key_len, 16)
                cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
                dec = cipher.decryptor()
                padded = dec.update(ciphertext) + dec.finalize()
                unpadder = padding.PKCS7(128).unpadder()
                plain = unpadder.update(padded) + unpadder.finalize()
            except Exception:
                continue
            if _bytes_looks_like_decrypted_html(plain):
                return plain
    return None


def _try_browser_submit_secure_mail_html(source_path: Path | None, password: str) -> str | None:
    if source_path is None or not source_path.exists():
        return None
    try:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception:
        return None

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.goto(source_path.resolve().as_uri(), wait_until="load", timeout=7000)
                password_input = page.locator("input[type='password']").first
                if password_input.count() == 0:
                    return None
                password_input.fill(password, timeout=3000)

                clicked = False
                for selector in (
                    "button[type='submit']",
                    "input[type='submit']",
                    "button:has-text('인증하기')",
                    "button:has-text('본문 보기')",
                    "button:has-text('열기')",
                    "button:has-text('확인')",
                    "input[value='인증하기']",
                    "input[value='확인']",
                ):
                    locator = page.locator(selector).first
                    try:
                        if locator.count() == 0:
                            continue
                        locator.click(timeout=2000)
                        clicked = True
                        break
                    except PlaywrightError:
                        continue

                if not clicked:
                    form_submitted = page.evaluate(
                        """
                        () => {
                          const input = document.querySelector("input[type='password']");
                          const form = input ? input.closest("form") : null;
                          if (form) {
                            form.requestSubmit ? form.requestSubmit() : form.submit();
                            return true;
                          }
                          return false;
                        }
                        """
                    )
                    if not form_submitted:
                        try:
                            password_input.press("Enter", timeout=1000)
                        except PlaywrightError:
                            pass

                try:
                    page.wait_for_function(
                        "() => !document.querySelector(\"input[type='password']\")",
                        timeout=4000,
                    )
                except PlaywrightTimeoutError:
                    page.wait_for_timeout(1500)

                rendered = page.content()
                if rendered and rendered != source_path.read_text(encoding="utf-8", errors="ignore"):
                    return rendered
                if rendered and not _looks_like_js_secure_mail_shell(rendered):
                    return rendered
            finally:
                browser.close()
    except Exception as exc:
        logger.warning("Browser secure-mail fallback failed: %s", exc)
        return None

    return None


def _extract_base64_cipher_candidates(html: str) -> list[str]:
    script_blocks = re.findall(r"<script\b[^>]*>(.*?)</script>", html, flags=re.IGNORECASE | re.DOTALL)
    blobs = [*script_blocks, html]
    raw_candidates: list[str] = []

    for blob in blobs:
        raw_candidates.extend(re.findall(r"[A-Za-z0-9+/][A-Za-z0-9+/\s\n=]{31,}={0,2}", blob))
        for match in _JS_STRING_CONCAT_RE.finditer(blob):
            joined = "".join(_iter_js_string_literals(match.group(0)))
            if joined:
                raw_candidates.append(joined)
        raw_candidates.extend(_iter_js_string_literals(blob))

    cleaned: list[str] = []
    seen: set[str] = set()
    for chunk in raw_candidates:
        for variant in _candidate_variants(chunk):
            if not _looks_like_base64_cipher_candidate(variant):
                continue
            if variant in seen:
                continue
            seen.add(variant)
            cleaned.append(variant)

    return sorted(
        cleaned,
        key=lambda value: (0 if value.startswith("U2FsdGVkX1") else 1, -len(value)),
    )


def _looks_like_js_secure_mail_shell(html: str) -> bool:
    if "Salted__" in html and re.search(r"[A-Za-z0-9+/]{32,}={0,2}", html):
        return True
    has_mail = "보안메일" in html or "인증메일" in html
    has_pw_label = "인증암호" in html or "인증 암호" in html
    hl = html.lower().replace(" ", "")
    has_crypto = ("crypto-js" in hl or "cryptojs" in hl or "aes.decrypt" in hl or "decrypt(" in hl)
    has_password_input = bool(
        re.search(
            r"<input\b[^>]*type\s*=\s*(['\"]?)password\1[^>]*>",
            html,
            flags=re.IGNORECASE,
        )
    )
    has_auth_action = bool(re.search(r"인증하기|인증 하기|본문 보기|보안메일 보기|열람하기", html))
    has_security_notice = ("주민번호 앞 6자리" in html or "소유자가 아닐 경우" in html or "본인 인증" in html)
    has_cipher_blob = any(candidate.startswith("U2FsdGVkX1") for candidate in _extract_base64_cipher_candidates(html))
    if has_mail and has_pw_label and has_crypto:
        return True
    if has_pw_label and has_crypto and ("인증하기" in html or "인증 하기" in html):
        return True
    if has_password_input and (has_mail or has_pw_label or has_security_notice) and (
        has_crypto or has_auth_action or has_cipher_blob
    ):
        return True
    if has_password_input and has_crypto and has_cipher_blob:
        return True
    return False


def _decrypt_js_secure_mail_html_if_applicable(
    html_text: str,
    password: str | None,
    *,
    source_path: Path | None = None,
) -> str:
    if not _looks_like_js_secure_mail_shell(html_text):
        return html_text

    pwd = (password or "").strip()
    if not pwd:
        raise ValueError(
            "보안메일(인증암호 입력) 형식의 HTML입니다. "
            "거래처 지정 비밀번호(designated_password)에 인증암호를 설정하세요."
        )

    candidates = _extract_base64_cipher_candidates(html_text)
    logger.debug("Secure-mail cipher candidates found: %s", len(candidates))
    for candidate in candidates:
        plain = _decrypt_openssl_salted_aes_b64(candidate, pwd)
        if plain:
            try:
                decoded = plain.decode("utf-8")
            except UnicodeDecodeError:
                decoded = plain.decode("cp949", errors="replace")
            logger.info("Decrypted JS secure-mail HTML wrapper (CryptoJS/OpenSSL-compatible AES)")
            return decoded

    browser_rendered = _try_browser_submit_secure_mail_html(source_path, pwd)
    if browser_rendered:
        logger.info("Decrypted secure-mail HTML via browser input/submit fallback")
        return browser_rendered

    logger.warning("Failed to decrypt secure-mail HTML after static decrypt and browser submit fallback.")
    raise ValueError(
        "보안메일 HTML 복호화에 실패했습니다. "
        "비밀번호가 올바른지 확인하거나, 브라우저 입력/submit 폴백까지 실패한 형식일 수 있습니다. "
        "CryptoJS AES(OpenSSL Salted__)가 아닌 형식이거나 추가 런타임 리소스가 필요할 수 있습니다."
    )


class HtmlDocumentLoaderMixin:
    """HTML 원문을 공통 raw_text 형식으로 바꾸는 보조 로직.

    HTML은 DOM 구조를 그대로 활용할 수 있다는 장점이 있지만,
    보험사/운용사 산출물은 rowspan/colspan 과 legacy encoding 이 섞여 있어
    단순 text 추출만으로는 표 구조가 쉽게 깨진다.
    """

    HTML_TABLE_RE = re.compile(r"<table\b.*?</table>", flags=re.IGNORECASE | re.DOTALL)
    HTML_ROW_RE = re.compile(r"<tr\b.*?</tr>", flags=re.IGNORECASE | re.DOTALL)
    HTML_CELL_RE = re.compile(r"<t([hd])\b([^>]*)>(.*?)</t[hd]>", flags=re.IGNORECASE | re.DOTALL)
    HTML_BREAK_RE = re.compile(r"<br\s*/?>", flags=re.IGNORECASE)
    HTML_TAG_RE = re.compile(r"<[^>]+>")
    HTML_BLOCK_END_RE = re.compile(r"</(div|p|li|section|article|header|footer|h[1-6])\s*>", flags=re.IGNORECASE)
    HTML_CHARSET_RE = re.compile(r"charset\s*=\s*['\"]?\s*([A-Za-z0-9._-]+)", flags=re.IGNORECASE)

    def _load_html(self, file_path: Path, html_password: str | None = None) -> str:
        """HTML 본문에서 일반 텍스트와 표를 분리해 공통 raw_text 형식으로 만든다.

        의도는 PDF/Excel 과 동일한 후단 파이프라인을 타게 만드는 것이다.
        즉 HTML도 최종적으로는 `[HTML 파일명] + text lines + pipe table` 형태로 맞춘다.
        """
        raw_text, _ = self._load_html_with_render_hints(file_path, html_password=html_password)
        return raw_text

    def _load_html_with_render_hints(
        self,
        file_path: Path,
        html_password: str | None = None,
    ) -> tuple[str, dict[str, object]]:
        """HTML raw_text와 markdown 렌더링용 힌트를 함께 만든다.

        raw_text는 LLM backup/coverage 기준으로 그대로 보존하고,
        markdown만 `rowspan`에서 상속된 셀을 더 보수적으로 다루기 위해
        별도 render hint를 함께 반환한다.
        """
        html_text = self._read_html_text(file_path, password=html_password)
        return self._extract_html_raw_text_with_render_hints(
            html_text,
            source_label=file_path.name,
            section_label="HTML",
        )

    def _extract_html_raw_text(self, html_text: str, source_label: str, section_label: str) -> str:
        """HTML 문자열을 공통 raw_text 형식으로 변환한다.

        파일에서 직접 읽은 HTML뿐 아니라 EML 본문 안의 HTML 파트도 같은 로직을
        재사용할 수 있게 별도 helper 로 분리했다.
        """
        raw_text, _ = self._extract_html_raw_text_with_render_hints(
            html_text,
            source_label=source_label,
            section_label=section_label,
        )
        return raw_text

    def _extract_html_raw_text_with_render_hints(
        self,
        html_text: str,
        *,
        source_label: str,
        section_label: str,
    ) -> tuple[str, dict[str, object]]:
        """HTML 문자열을 raw_text와 markdown render hint로 함께 변환한다."""
        body_html = html_text
        soup = self._parse_html_with_bs4(html_text)
        if soup is not None:
            container = soup.body or soup
            serialized_body = "".join(str(child) for child in container.contents).strip()
            if serialized_body:
                body_html = serialized_body
        else:
            body_match = re.search(r"<body\b[^>]*>(.*?)</body>", html_text, flags=re.IGNORECASE | re.DOTALL)
            body_html = body_match.group(1) if body_match else html_text
        lines: list[str] = [f"[{section_label} {source_label}]"]
        markdown_blocks: list[str] = []
        pipe_table_hints: list[dict[str, object]] = []
        label_tokens: list[str] = []
        table_row_tokens: list[str] = []
        inherited_row_tokens: list[str] = []
        row_kind_tokens: list[str] = []
        fund_code_tokens: list[str] = []
        canonical_blocks = self._extract_html_canonical_blocks(body_html)

        for block in canonical_blocks:
            block_lines: list[str]
            if isinstance(block, HtmlTextBlockProjection):
                block_lines = list(block.lines)
                markdown_block = self._render_text_block(block.lines)
                if markdown_block:
                    markdown_blocks.append(markdown_block)
                label_tokens.extend(line for line in block.lines if line.strip())
            else:
                block_lines, table_hint = self._extract_html_table_lines_with_hints_from_projection(block)
                if not block_lines:
                    continue
                pipe_table_hints.append(table_hint)
                markdown_block = self._render_lossless_html_table(block)
                if markdown_block:
                    markdown_blocks.append(markdown_block)
                    markdown_lines = [line.strip() for line in markdown_block.splitlines() if line.strip().startswith("|")]
                    table_row_tokens.extend(markdown_lines)
                    inherited_row_tokens.extend(self._html_inherited_markdown_rows(block, markdown_lines))
                    row_kind_tokens.extend(self._html_row_kind_tokens(block))
                    fund_code_tokens.extend(self._html_fund_code_tokens(block))
                    label_tokens.extend(self._html_table_label_tokens(markdown_lines))
            if not block_lines:
                continue
            if len(lines) > 1 and lines[-1] != "":
                lines.append("")
            lines.extend(block_lines)

        meaningful_lines = [line for line in lines if line.strip()]
        logger.info(
            "Extracted text from HTML document: block_count=%s table_count=%s",
            len(canonical_blocks),
            len(pipe_table_hints),
        )
        if len(meaningful_lines) <= 1:
            raise ValueError("No extractable text found in HTML document.")
        render_hints: dict[str, object] = {
            "pipe_table_hints": pipe_table_hints,
            "markdown_blocks": markdown_blocks,
            "html_markdown_audit": {
                "label_tokens": self._dedupe_preserve_order(label_tokens),
                "table_row_tokens": self._dedupe_preserve_order(table_row_tokens),
                "inherited_row_tokens": self._dedupe_preserve_order(inherited_row_tokens),
                "row_kind_tokens": self._dedupe_preserve_order(row_kind_tokens),
                "fund_code_tokens": self._dedupe_preserve_order(fund_code_tokens),
            },
        }
        render_hints["preferred_markdown_text"] = self._compose_html_section_markdown(
            section_label=section_label,
            source_label=source_label,
            preamble_lines=[],
            markdown_blocks=markdown_blocks,
        )
        return "\n".join(lines).strip(), render_hints

    @staticmethod
    def _dedupe_preserve_order(values: list[str]) -> list[str]:
        deduped: list[str] = []
        seen: set[str] = set()
        for value in values:
            normalized = value.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped

    def _compose_html_section_markdown(
        self,
        *,
        section_label: str,
        source_label: str,
        preamble_lines: list[str],
        markdown_blocks: list[str],
    ) -> str:
        section_header = f"[{section_label} {source_label}]"
        if hasattr(self, "_format_markdown_heading"):
            heading = self._format_markdown_heading(section_header, 1)
        else:  # pragma: no cover - standalone fallback
            heading = f"## {section_label} {source_label}"

        blocks: list[str] = []
        preamble_block = self._render_text_block(preamble_lines)
        if preamble_block:
            blocks.append(preamble_block)
        blocks.extend(block for block in markdown_blocks if block.strip())
        if not blocks:
            return heading
        return f"{heading}\n\n" + "\n\n".join(blocks)

    def _extract_html_canonical_blocks(
        self,
        html_fragment: str,
    ) -> list[HtmlTextBlockProjection | HtmlTableBlockProjection]:
        soup = self._parse_html_with_bs4(html_fragment)
        if soup is None:
            return self._extract_html_canonical_blocks_via_regex(html_fragment)

        container = soup.body or soup
        serialized = "".join(str(child) for child in container.contents)
        table_blocks: list[tuple[str, HtmlTableBlockProjection]] = []

        # Nested layout tables are common in insurer HTML. Only replacing leaf tables
        # avoids swallowing the real data table into an outer wrapper block.
        for index, table_tag in enumerate(list(container.find_all("table"))):
            if table_tag.find("table") is not None:
                continue
            projection = self._build_html_table_block_from_tag(table_tag)
            if projection is None:
                continue
            marker = f"__VA_HTML_TABLE_BLOCK_{index}__"
            table_blocks.append((marker, projection))
            table_tag.replace_with(marker)

        serialized = "".join(str(child) for child in container.contents)
        blocks: list[HtmlTextBlockProjection | HtmlTableBlockProjection] = []
        for marker, projection in table_blocks:
            text_fragment, found, remainder = serialized.partition(marker)
            if not found:
                continue
            text_block = self._build_html_text_block(text_fragment)
            if text_block is not None:
                blocks.append(text_block)
            blocks.append(projection)
            serialized = remainder

        tail_block = self._build_html_text_block(serialized)
        if tail_block is not None:
            blocks.append(tail_block)
        return blocks or self._extract_html_canonical_blocks_via_regex(html_fragment)

    def _extract_html_canonical_blocks_via_regex(
        self,
        html_fragment: str,
    ) -> list[HtmlTextBlockProjection | HtmlTableBlockProjection]:
        blocks: list[HtmlTextBlockProjection | HtmlTableBlockProjection] = []
        cursor = 0
        for table_match in self.HTML_TABLE_RE.finditer(html_fragment):
            text_fragment = html_fragment[cursor:table_match.start()]
            text_block = self._build_html_text_block(text_fragment)
            if text_block is not None:
                blocks.append(text_block)
            table_block = self._build_html_table_block_via_regex(table_match.group(0))
            if table_block is not None:
                blocks.append(table_block)
            cursor = table_match.end()

        tail_block = self._build_html_text_block(html_fragment[cursor:])
        if tail_block is not None:
            blocks.append(tail_block)
        return blocks

    def _build_html_text_block(self, html_fragment: str) -> HtmlTextBlockProjection | None:
        lines = self._extract_html_text_lines(html_fragment)
        if not lines:
            return None
        return HtmlTextBlockProjection(lines=lines)

    def _extract_html_text_lines(self, html_fragment: str) -> list[str]:
        """표 바깥의 일반 본문 텍스트를 줄 단위로 추출한다.

        HTML은 태그를 지우는 순서에 따라 문장 경계가 사라지기 쉽다.
        먼저 줄 경계가 될 태그를 `\n`으로 치환하고, 그 다음 태그를 벗겨서
        본문 line list를 복원한다.
        """
        normalized = self.HTML_BREAK_RE.sub("\n", html_fragment)
        normalized = self.HTML_BLOCK_END_RE.sub("\n", normalized)
        normalized = re.sub(r"<(script|style)\b.*?</\1>", "", normalized, flags=re.IGNORECASE | re.DOTALL)
        normalized = self.HTML_TAG_RE.sub(" ", normalized)
        normalized = unescape(normalized)

        lines: list[str] = []
        for raw_line in normalized.splitlines():
            line = re.sub(r"\s+", " ", raw_line).strip()
            if not line:
                continue
            if not lines or lines[-1] != line:
                lines.append(line)
        return lines

    def _extract_html_table_lines(self, table_html: str) -> list[str]:
        """HTML 표를 후단 공통 로직이 읽을 수 있는 pipe row로 변환한다."""
        expanded_rows = self._expand_html_table(table_html)
        return [" | ".join(row) for row in expanded_rows if any(cell for cell in row)]

    def _extract_html_table_lines_with_hints(self, table_html: str) -> tuple[list[str], dict[str, object]]:
        """HTML 표를 pipe row와 inherited-cell 힌트로 함께 변환한다."""
        projection = self._build_html_table_block(table_html)
        if projection is None:
            return [], {"inherited_rows": []}
        return self._extract_html_table_lines_with_hints_from_projection(projection)

    def _extract_html_table_lines_with_hints_from_projection(
        self,
        projection: HtmlTableBlockProjection,
    ) -> tuple[list[str], dict[str, object]]:
        expanded_rows = [[cell.text for cell in row.cells] for row in projection.rows]
        inherited_rows = [[cell.is_inherited for cell in row.cells] for row in projection.rows]
        table_lines = [" | ".join(row) for row in expanded_rows if any(cell for cell in row)]
        filtered_inherited_rows = [
            row
            for row_index, row in enumerate(inherited_rows)
            if row_index < len(expanded_rows) and any(cell for cell in expanded_rows[row_index])
        ]
        return table_lines, {"inherited_rows": filtered_inherited_rows}

    def _expand_html_table(self, table_html: str) -> list[list[str]]:
        """`rowspan/colspan`을 전개해 시각적으로 보이는 표를 복원한다.

        후단 coverage와 markdown 변환은 "행/열이 명확한 2차원 배열"을 기대한다.
        HTML의 병합 셀은 이 형태와 다르기 때문에, 여기서 먼저 평면 테이블로 바꿔 둔다.
        """
        expanded_rows, _ = self._expand_html_table_with_inherited_mask(table_html)
        return expanded_rows

    def _expand_html_table_with_inherited_mask(self, table_html: str) -> tuple[list[list[str]], list[list[bool]]]:
        """`rowspan/colspan` 전개와 함께 inherited-cell mask를 만든다."""
        projection = self._build_html_table_block(table_html)
        if projection is None:
            return [], []
        expanded_rows = [[cell.text for cell in row.cells] for row in projection.rows]
        inherited_rows = [[cell.is_inherited for cell in row.cells] for row in projection.rows]
        return expanded_rows, inherited_rows

    def _build_html_table_block(self, table_html: str) -> HtmlTableBlockProjection | None:
        soup = self._parse_html_with_bs4(table_html)
        if soup is not None:
            table_tag = soup.find("table")
            if table_tag is not None:
                projection = self._build_html_table_block_from_tag(table_tag)
                if projection is not None:
                    return projection
        return self._build_html_table_block_via_regex(table_html)

    def _parse_html_with_bs4(self, html_text: str):
        if BeautifulSoup is None:
            return None
        try:
            return BeautifulSoup(html_text, "lxml")
        except Exception:
            logger.warning("BeautifulSoup HTML parsing failed; falling back to regex extraction.")
            return None

    def _build_html_table_block_via_regex(self, table_html: str) -> HtmlTableBlockProjection | None:
        row_groups = [
            self._parse_html_cells_with_metadata(row_html)
            for row_html in self.HTML_ROW_RE.findall(table_html)
        ]
        return self._project_html_table_rows(row_groups)

    def _build_html_table_block_from_tag(self, table_tag) -> HtmlTableBlockProjection | None:
        row_groups = [
            self._parse_html_cells_with_tag_metadata_from_tag(row_tag, table_tag)
            for row_tag in self._iter_html_table_rows(table_tag)
        ]
        return self._project_html_table_rows(row_groups)

    def _project_html_table_rows(
        self,
        row_groups: list[list[tuple[str, str, int, int]]],
    ) -> HtmlTableBlockProjection | None:
        pending_cells: dict[int, tuple[HtmlTableCellProjection, int]] = {}
        expanded_rows: list[HtmlTableRowProjection] = []
        max_columns = 0

        for source_row, row_cells in enumerate(row_groups):
            if not row_cells:
                continue

            expanded_row: list[HtmlTableCellProjection] = []
            column_index = 0

            def drain_pending() -> None:
                nonlocal column_index
                while column_index in pending_cells:
                    base_cell, remaining_rows = pending_cells[column_index]
                    expanded_row.append(
                        HtmlTableCellProjection(
                            text=base_cell.text,
                            rowspan=base_cell.rowspan,
                            colspan=base_cell.colspan,
                            source_row=base_cell.source_row,
                            source_col=base_cell.source_col,
                            is_inherited=True,
                            row_kind=base_cell.row_kind,
                        )
                    )
                    if remaining_rows <= 1:
                        del pending_cells[column_index]
                    else:
                        pending_cells[column_index] = (base_cell, remaining_rows - 1)
                    column_index += 1

            row_is_header = all(cell_kind == "header" for cell_kind, *_rest in row_cells)
            for source_col, (cell_kind, text, rowspan, colspan) in enumerate(row_cells):
                drain_pending()
                safe_colspan = max(1, colspan)
                safe_rowspan = max(1, rowspan)
                for _ in range(safe_colspan):
                    cell = HtmlTableCellProjection(
                        text=text,
                        rowspan=safe_rowspan,
                        colspan=safe_colspan,
                        source_row=source_row,
                        source_col=source_col,
                        is_inherited=False,
                    )
                    expanded_row.append(cell)
                    if safe_rowspan > 1:
                        pending_cells[column_index] = (cell, safe_rowspan - 1)
                    column_index += 1

            drain_pending()
            if not any(cell.text for cell in expanded_row):
                continue

            row_kind = None if row_is_header else self._infer_html_row_kind(expanded_row)
            for cell in expanded_row:
                cell.row_kind = row_kind
            max_columns = max(max_columns, len(expanded_row))
            expanded_rows.append(
                HtmlTableRowProjection(
                    cells=expanded_row,
                    is_header=row_is_header,
                    row_kind=row_kind,
                )
            )

        if not expanded_rows:
            return None

        for row_index, row in enumerate(expanded_rows):
            while len(row.cells) < max_columns:
                row.cells.append(
                    HtmlTableCellProjection(
                        text="",
                        rowspan=1,
                        colspan=1,
                        source_row=row_index,
                        source_col=len(row.cells),
                        is_inherited=False,
                        row_kind=row.row_kind,
                    )
                )
        return HtmlTableBlockProjection(rows=expanded_rows)

    @staticmethod
    def _iter_html_table_rows(table_tag):
        for row_tag in table_tag.find_all("tr"):
            if row_tag.find_parent("table") is table_tag:
                yield row_tag

    @staticmethod
    def _iter_html_row_cells(row_tag, table_tag):
        direct_cells = row_tag.find_all(["th", "td"], recursive=False)
        if direct_cells:
            for cell_tag in direct_cells:
                if cell_tag.find_parent("table") is table_tag:
                    yield cell_tag
            return

        for cell_tag in row_tag.find_all(["th", "td"]):
            if cell_tag.find_parent("tr") is not row_tag:
                continue
            if cell_tag.find_parent("table") is not table_tag:
                continue
            yield cell_tag

    def _parse_html_cells(self, row_html: str) -> list[tuple[str, int, int]]:
        """`<tr>` 내부의 각 셀을 `(text, rowspan, colspan)` 튜플로 읽는다."""
        parsed_cells: list[tuple[str, int, int]] = []
        for _, attrs, cell_html in self.HTML_CELL_RE.findall(row_html):
            text = self._html_cell_text(cell_html)
            rowspan = self._html_span_value(attrs, "rowspan")
            colspan = self._html_span_value(attrs, "colspan")
            parsed_cells.append((text, rowspan, colspan))
        return parsed_cells

    def _parse_html_cells_with_metadata(
        self,
        row_html: str,
    ) -> list[tuple[str, str, int, int]]:
        parsed_cells: list[tuple[str, str, int, int]] = []
        for cell_tag, attrs, cell_html in self.HTML_CELL_RE.findall(row_html):
            text = self._html_cell_text(cell_html)
            rowspan = self._html_span_value(attrs, "rowspan")
            colspan = self._html_span_value(attrs, "colspan")
            parsed_cells.append(("header" if cell_tag.lower() == "h" else "body", text, rowspan, colspan))
        return parsed_cells

    def _parse_html_cells_with_tag_metadata_from_tag(
        self,
        row_tag,
        table_tag,
    ) -> list[tuple[str, str, int, int]]:
        parsed_cells: list[tuple[str, str, int, int]] = []
        for cell_tag in self._iter_html_row_cells(row_tag, table_tag):
            text = self._html_cell_text_from_tag(cell_tag)
            rowspan = self._html_tag_span_value(cell_tag, "rowspan")
            colspan = self._html_tag_span_value(cell_tag, "colspan")
            parsed_cells.append(("header" if cell_tag.name.lower() == "th" else "body", text, rowspan, colspan))
        return parsed_cells

    def _infer_html_row_kind(self, cells: list[HtmlTableCellProjection]) -> str | None:
        detector = getattr(self, "_looks_like_order_context_value", None)
        if detector is None:
            return None
        for cell in cells:
            value = cell.text.strip()
            if not value:
                continue
            try:
                if detector(value):
                    return value
            except Exception:  # pragma: no cover - defensive for standalone copies
                return None
        return None

    def _render_lossless_html_table(self, block: HtmlTableBlockProjection) -> str:
        rows = [[cell.text for cell in row.cells] for row in block.rows]
        if not rows:
            return ""

        header_row_count = 0
        for row in block.rows:
            if row.is_header:
                header_row_count += 1
                continue
            break

        if 0 < header_row_count < len(rows):
            header_rows = rows[:header_row_count]
            body_rows = rows[header_row_count:]
        else:
            normalized_rows = self._normalize_pipe_rows(rows)
            table_structure = self._infer_table_structure(normalized_rows)
            if table_structure is None:
                return self._render_text_block([" | ".join(row) for row in rows if any(cell for cell in row)])
            _, header_start, data_start = table_structure
            header_rows = normalized_rows[header_start:data_start]
            body_rows = normalized_rows[data_start:]

        if not header_rows:
            header = [f"col_{index + 1}" for index in range(len(rows[0]))]
        else:
            header = self._collapse_header_rows(self._normalize_pipe_rows(header_rows))
            header = self._normalize_lossless_html_headers(header, body_rows)

        markdown_lines = [
            f"| {' | '.join(self._escape_markdown_cell(cell or f'col_{index + 1}') for index, cell in enumerate(header))} |",
            f"| {' | '.join('---' for _ in range(len(header)))} |",
        ]
        for row in body_rows:
            markdown_lines.append(f"| {' | '.join(self._escape_markdown_cell(cell) for cell in row)} |")
        return "\n".join(markdown_lines)

    def _normalize_lossless_html_headers(self, header: list[str], body_rows: list[list[str]]) -> list[str]:
        normalized = list(header)
        for index, label in enumerate(normalized):
            if self._is_order_context_label(label):
                continue
            if not self._column_has_order_context_values(body_rows, index):
                continue
            if label:
                label_segments = {segment.strip() for segment in label.split("/") if segment.strip()}
                if "구분" in label_segments:
                    continue
                normalized[index] = f"{label} / 구분"
            else:
                normalized[index] = "구분"
        return normalized

    def _html_inherited_markdown_rows(
        self,
        block: HtmlTableBlockProjection,
        markdown_lines: list[str],
    ) -> list[str]:
        inherited_rows: list[str] = []
        body_row_start = 2
        markdown_body_lines = markdown_lines[body_row_start:]
        body_rows = [row for row in block.rows if not row.is_header]
        for row, markdown_line in zip(body_rows, markdown_body_lines):
            if any(cell.is_inherited and cell.text for cell in row.cells):
                inherited_rows.append(markdown_line)
        return inherited_rows

    def _html_row_kind_tokens(self, block: HtmlTableBlockProjection) -> list[str]:
        return [
            row.row_kind
            for row in block.rows
            if not row.is_header and row.row_kind
        ]

    def _html_fund_code_tokens(self, block: HtmlTableBlockProjection) -> list[str]:
        candidate_codes: list[str] = []
        for row in block.rows:
            if row.is_header:
                continue
            for cell in row.cells[:8]:
                value = cell.text.strip()
                if not value or self._is_total_like_text(value):
                    continue
                if self._looks_like_fund_code(value):
                    candidate_codes.append(value)
                    break

        unique_codes = self._dedupe_preserve_order(candidate_codes)
        if not unique_codes:
            return []

        indices = (0, len(unique_codes) // 2, len(unique_codes) - 1)
        tokens: list[str] = []
        for index in indices:
            token = unique_codes[index]
            if token not in tokens:
                tokens.append(token)
        return tokens

    def _html_table_label_tokens(self, markdown_lines: list[str]) -> list[str]:
        if not markdown_lines:
            return []
        return [markdown_lines[0]]

    def _html_cell_text_from_tag(self, cell_tag) -> str:
        return self._html_cell_text(cell_tag.decode_contents())

    def _html_cell_text(self, cell_html: str) -> str:
        """셀 내부 HTML을 사람이 읽을 수 있는 한 줄 문자열로 정리한다."""
        normalized = self.HTML_BREAK_RE.sub(" / ", cell_html)
        normalized = self.HTML_TAG_RE.sub(" ", normalized)
        normalized = unescape(normalized)
        return re.sub(r"\s+", " ", normalized).strip()

    @staticmethod
    def _html_tag_span_value(cell_tag, name: str) -> int:
        raw_value = cell_tag.get(name)
        if raw_value is None:
            return 1
        try:
            return max(1, int(str(raw_value).strip()))
        except (TypeError, ValueError):
            return 1

    def _read_html_text(self, file_path: Path, password: str | None = None) -> str:
        """HTML을 utf-8 우선, 필요 시 한국어 legacy encoding fallback 으로 읽는다.

        운영 환경에서는 cp949/euc-kr HTML이 실제로 들어오므로,
        로더 단계에서 인코딩 때문에 실패하지 않게 최대한 관대하게 처리한다.
        """
        raw = file_path.read_bytes()
        zip_html = self._try_read_html_bytes_from_zip(raw, password=password)
        extracted_secure_mail_tmpdir: tempfile.TemporaryDirectory[str] | None = None
        if zip_html is not None:
            logger.info(
                "Decoded HTML from ZIP-in-HTML container: path=%s bytes=%s",
                file_path,
                len(zip_html),
            )
            text = self._decode_html_bytes(zip_html)
            secure_mail_source_path, extracted_secure_mail_tmpdir = (
                self._materialize_zip_html_bundle_for_browser(raw, password=password)
                if _looks_like_js_secure_mail_shell(text)
                else (None, None)
            )
        else:
            text = self._decode_html_bytes(raw)
            secure_mail_source_path = file_path
        try:
            return _decrypt_js_secure_mail_html_if_applicable(
                text,
                password,
                source_path=secure_mail_source_path,
            )
        finally:
            if extracted_secure_mail_tmpdir is not None:
                extracted_secure_mail_tmpdir.cleanup()

    def _try_read_html_bytes_from_zip(self, raw: bytes, password: str | None) -> bytes | None:
        if len(raw) < 4 or raw[:2] != b"PK":
            return None
        try:
            zf = zipfile.ZipFile(io.BytesIO(raw))
        except zipfile.BadZipFile:
            return None
        try:
            html_names = [n for n in zf.namelist() if n.lower().endswith((".html", ".htm"))]
            if not html_names:
                raise ValueError(
                    "ZIP 형태로 보이는 파일에 .html/.htm 항목이 없습니다. "
                    "웹페이지 저장 묶음이 맞는지 확인하세요."
                )
            html_names.sort(key=self._html_zip_member_sort_key)
            target = html_names[0]
            try:
                return self._read_zip_member_bytes(zf, target, password=password)
            except (RuntimeError, OSError, zipfile.BadZipFile, zlib.error, EOFError, NotImplementedError) as exc:
                self._raise_zip_html_read_failure(zf, target, exc)
        finally:
            zf.close()

    def _materialize_zip_html_bundle_for_browser(
        self,
        raw: bytes,
        password: str | None,
    ) -> tuple[Path | None, tempfile.TemporaryDirectory[str] | None]:
        if len(raw) < 4 or raw[:2] != b"PK":
            return None, None
        try:
            zf = zipfile.ZipFile(io.BytesIO(raw))
        except zipfile.BadZipFile:
            return None, None

        tmpdir = tempfile.TemporaryDirectory(prefix="va_secure_html_")
        base_dir = Path(tmpdir.name).resolve()
        try:
            html_names = [name for name in zf.namelist() if name.lower().endswith((".html", ".htm"))]
            if not html_names:
                tmpdir.cleanup()
                return None, None

            html_names.sort(key=self._html_zip_member_sort_key)
            target_name = html_names[0]
            target_path: Path | None = None

            for info in zf.infolist():
                member_name = info.filename
                if not member_name or member_name.endswith("/"):
                    continue
                dest_path = self._safe_zip_dest_path(base_dir, member_name)
                if dest_path is None:
                    logger.warning("Skipping unsafe ZIP member during browser fallback extraction: %s", member_name)
                    continue
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                member_bytes = self._read_zip_member_bytes(zf, member_name, password=password)
                dest_path.write_bytes(member_bytes)
                if member_name == target_name:
                    target_path = dest_path

            if target_path is None or not target_path.exists():
                tmpdir.cleanup()
                return None, None
            return target_path, tmpdir
        except Exception as exc:
            logger.warning("Failed to materialize ZIP HTML bundle for browser fallback: %s", exc)
            tmpdir.cleanup()
            return None, None
        finally:
            zf.close()

    @staticmethod
    def _safe_zip_dest_path(base_dir: Path, member_name: str) -> Path | None:
        normalized = member_name.replace("\\", "/").lstrip("/")
        if not normalized:
            return None
        dest_path = (base_dir / normalized).resolve()
        if dest_path != base_dir and base_dir not in dest_path.parents:
            return None
        return dest_path

    @staticmethod
    def _zip_password_variants(password: str | None) -> list[bytes | None]:
        password_str = (password or "").strip()
        if not password_str:
            return [None]

        variants: list[bytes | None] = []
        for enc in ("utf-8", "cp437", "latin-1"):
            try:
                pwd_bytes = password_str.encode(enc)
            except UnicodeEncodeError:
                continue
            if pwd_bytes not in variants:
                variants.append(pwd_bytes)
        return variants or [None]

    def _read_zip_member_bytes(
        self,
        zf: zipfile.ZipFile,
        member_name: str,
        *,
        password: str | None,
    ) -> bytes:
        last_exc: BaseException | None = None
        for pwd_bytes in self._zip_password_variants(password):
            try:
                return zf.read(member_name, pwd=pwd_bytes)
            except (RuntimeError, OSError, zipfile.BadZipFile, zlib.error, EOFError, NotImplementedError) as exc:
                last_exc = exc
        if last_exc is not None:
            raise last_exc
        return zf.read(member_name)

    def _raise_zip_html_read_failure(
        self,
        zf: zipfile.ZipFile,
        target: str,
        exc: BaseException | None,
    ) -> None:
        if exc is None:
            raise ValueError("ZIP HTML 항목을 읽지 못했습니다.")

        if isinstance(exc, NotImplementedError):
            raise ValueError(
                "이 ZIP은 현재 지원하지 않는 암호 방식(AES 등)일 수 있습니다. "
                "전통 ZipCrypto 방식으로 다시 저장해 보세요."
            ) from exc

        msg_l = str(exc).lower()
        if "aes" in msg_l or "unsupported compression" in msg_l:
            raise ValueError(
                "이 ZIP은 현재 환경에서 지원하지 않는 형식일 수 있습니다(AES 암호·압축 등)."
            ) from exc

        if (
            "bad password" in msg_l
            or "incorrect password" in msg_l
            or ("password" in msg_l and "required" in msg_l)
        ):
            raise ValueError(
                "암호가 걸린 ZIP HTML입니다. 거래처 지정 비밀번호(designated_password) 등을 확인하세요."
            ) from exc

        encrypted = False
        try:
            info = zf.getinfo(target)
            encrypted = bool(info.flag_bits & 0x1)
        except KeyError:
            pass

        if encrypted and isinstance(exc, (zlib.error, RuntimeError, EOFError, OSError)):
            raise ValueError(
                "ZIP HTML을 여는 데 실패했습니다. 비밀번호가 틀렸거나 파일이 손상되었을 수 있습니다. "
                "지정 비밀번호(designated_password)를 확인하세요."
            ) from exc

        raise exc

    @staticmethod
    def _html_zip_member_sort_key(name: str) -> tuple[int, int, int, str]:
        base = Path(name).name.lower()
        index_first = 0 if base in ("index.html", "index.htm") else 1
        return (index_first, name.count("/"), len(name), name)

    def _decode_html_bytes(self, raw_bytes: bytes, declared_charset: str | None = None) -> str:
        """원시 HTML bytes를 가장 그럴듯한 인코딩으로 복원한다.

        declared charset, BOM, 흔한 fallback 후보를 모두 시도하고,
        "HTML 마커가 살아 있고 텍스트가 덜 깨진" 결과를 고른다.
        잘못 선언된 charset 때문에 엉뚱한 디코딩이 먼저 성공하더라도
        더 자연스러운 후보가 있으면 그쪽을 선택한다.
        """
        decoded_candidates: list[tuple[int, int, str, str]] = []
        candidate_encodings = self._html_encoding_candidates(raw_bytes, declared_charset=declared_charset)

        for index, encoding in enumerate(candidate_encodings):
            try:
                html_text = raw_bytes.decode(encoding)
            except (LookupError, UnicodeDecodeError):
                continue
            normalized_text = html_text.lstrip("\ufeff")
            score = self._score_decoded_html(normalized_text)
            decoded_candidates.append((score, -index, encoding, normalized_text))

        if not decoded_candidates:
            raise ValueError("Failed to decode HTML document with supported encodings.")

        best_score, _, best_encoding, best_text = max(decoded_candidates)
        logger.info("Decoded HTML using encoding=%s score=%s", best_encoding, best_score)
        return best_text

    def _html_encoding_candidates(self, raw_bytes: bytes, declared_charset: str | None = None) -> list[str]:
        """디코딩에 시도할 인코딩 후보 목록을 우선순위 순으로 만든다."""
        candidates: list[str] = []
        bom_encoding = self._detect_bom_encoding(raw_bytes)
        if bom_encoding:
            candidates.append(bom_encoding)

        probe = raw_bytes[:4096].decode("latin-1", errors="ignore")
        if declared_charset:
            candidates.extend(self._expand_html_encoding_aliases(declared_charset))
        charset_match = self.HTML_CHARSET_RE.search(probe)
        if charset_match:
            candidates.extend(self._expand_html_encoding_aliases(charset_match.group(1)))
        if self._looks_like_utf16_bytes(raw_bytes):
            candidates.extend(["utf-16", "utf-16-le", "utf-16-be"])
        candidates.extend(["utf-8-sig", "utf-8", "cp949", "euc-kr", "utf-16", "utf-16-le", "utf-16-be", "latin-1"])
        deduped: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            normalized = candidate.strip().lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped

    @staticmethod
    def _detect_bom_encoding(raw_bytes: bytes) -> str | None:
        """BOM으로 드러나는 인코딩을 우선 감지한다."""
        if raw_bytes.startswith(codecs.BOM_UTF8):
            return "utf-8-sig"
        if raw_bytes.startswith(codecs.BOM_UTF16_LE) or raw_bytes.startswith(codecs.BOM_UTF16_BE):
            return "utf-16"
        return None

    @staticmethod
    def _looks_like_utf16_bytes(raw_bytes: bytes) -> bool:
        """널 바이트 패턴을 보고 UTF-16 가능성을 추정한다."""
        sample = raw_bytes[:200]
        if len(sample) < 4:
            return False
        even_nuls = sum(1 for index in range(0, len(sample), 2) if sample[index] == 0)
        odd_nuls = sum(1 for index in range(1, len(sample), 2) if sample[index] == 0)
        threshold = max(4, len(sample) // 12)
        return even_nuls >= threshold or odd_nuls >= threshold

    @staticmethod
    def _expand_html_encoding_aliases(encoding: str) -> list[str]:
        """charset 별칭을 파이썬 codec 후보 이름들로 확장한다."""
        normalized = encoding.strip().lower()
        alias_map = {
            "ks_c_5601-1987": ["cp949", "euc-kr", "ks_c_5601-1987"],
            "ksc5601": ["cp949", "euc-kr", "ksc5601"],
            "euckr": ["euc-kr", "cp949"],
            "cp-949": ["cp949"],
            "unicode": ["utf-16", "utf-16-le", "utf-16-be"],
            "utf16": ["utf-16"],
            "utf16le": ["utf-16-le"],
            "utf16be": ["utf-16-be"],
            "latin1": ["latin-1"],
        }
        return alias_map.get(normalized, [normalized])

    @staticmethod
    def _score_decoded_html(text: str) -> int:
        """디코딩 결과가 얼마나 "덜 깨졌는지"를 점수화한다.

        이 점수는 완전한 의미 이해가 아니라 후보 간 상대 비교용이다.
        태그 존재, 한글 수, 제어문자/깨진 문자 수 등을 종합해 가장 자연스러운 결과를 고른다.
        """
        lower_text = text.lower()
        score = 0
        for marker, weight in (
            ("<html", 12),
            ("<body", 10),
            ("<table", 10),
            ("<tr", 6),
            ("<td", 6),
            ("<th", 6),
            ("charset=", 2),
        ):
            if marker in lower_text:
                score += weight

        hangul_count = len(re.findall(r"[가-힣]", text))
        score += min(12, hangul_count)
        score -= text.count("\ufffd") * 20
        score -= text.count("\x00") * 10
        score -= len(re.findall(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", text)) * 2

        # latin-1로 잘못 읽힌 cp949/euc-kr 문서는 태그는 살아 있어도 한글이 거의 없고
        # 확장 라틴 문자만 길게 이어지는 경우가 많다.
        if hangul_count == 0 and re.search(r"[À-ÿ]{4,}", text):
            score -= 8
        return score

    @staticmethod
    def _html_span_value(attrs: str, name: str) -> int:
        """셀 속성 문자열에서 `rowspan/colspan` 값을 읽고 없으면 1을 반환한다."""
        match = re.search(fr'{name}\s*=\s*["\']?(\d+)', attrs, flags=re.IGNORECASE)
        if not match:
            return 1
        return int(match.group(1))
