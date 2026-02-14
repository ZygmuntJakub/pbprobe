/// Normalize SQL into a fingerprint by replacing literals with placeholders.
///
/// - String literals 'foo' → $S
/// - Numeric literals → $N
/// - IN (...) lists → IN ($...)
/// - Lowercases SQL keywords (rough heuristic: lowercases everything)
pub fn fingerprint(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        match bytes[i] {
            // String literal
            b'\'' => {
                result.push_str("$S");
                i += 1;
                // Skip until closing quote, handling escaped quotes ''
                while i < len {
                    if bytes[i] == b'\'' {
                        i += 1;
                        if i < len && bytes[i] == b'\'' {
                            // Escaped quote, continue
                            i += 1;
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            // Dollar-quoted string $tag$...$tag$
            b'$' if i + 1 < len && (bytes[i + 1] == b'$' || bytes[i + 1].is_ascii_alphabetic() || bytes[i + 1] == b'_') => {
                // Check if this is a dollar-quoted string or a parameter placeholder
                if let Some(tag_end) = find_dollar_tag_end(bytes, i) {
                    let tag = &sql[i..=tag_end];
                    result.push_str("$S");
                    i = tag_end + 1;
                    // Find closing tag
                    while i + tag.len() <= len {
                        if &sql[i..i + tag.len()] == tag {
                            i += tag.len();
                            break;
                        }
                        i += 1;
                    }
                } else {
                    // Parameter placeholder like $1, $2
                    result.push(bytes[i] as char);
                    i += 1;
                }
            }
            // Numeric literal
            b'0'..=b'9' => {
                // Check if preceded by an identifier char (part of a name, not a number)
                let prev_is_ident = i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
                if prev_is_ident {
                    result.push(bytes[i] as char);
                    i += 1;
                } else {
                    result.push_str("$N");
                    // Skip the whole number (including decimals)
                    while i < len && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
                        i += 1;
                    }
                }
            }
            // Everything else
            ch => {
                result.push(ch as char);
                i += 1;
            }
        }
    }

    // Normalize IN ($N, $N, ...) → IN ($...)
    normalize_in_lists(&result).to_lowercase()
}

fn find_dollar_tag_end(bytes: &[u8], start: usize) -> Option<usize> {
    // $$ or $tag$ — find the second $
    let mut i = start + 1;
    if i < bytes.len() && bytes[i] == b'$' {
        return Some(i);
    }
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    if i < bytes.len() && bytes[i] == b'$' {
        Some(i)
    } else {
        None
    }
}

fn normalize_in_lists(sql: &str) -> String {
    // Replace IN ($N, $N, $N) or IN ($S, $S) with IN ($...)
    // Simple regex-like approach
    let mut result = String::with_capacity(sql.len());
    let upper = sql.to_uppercase();
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        // Look for "IN" followed by whitespace and "("
        if i + 2 < len && &upper[i..i + 2] == "IN" && (i == 0 || !bytes[i - 1].is_ascii_alphanumeric()) {
            let mut j = i + 2;
            // Skip whitespace
            while j < len && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < len && bytes[j] == b'(' {
                // Check if the content is only $N/$S separated by commas and spaces
                j += 1;
                let mut all_placeholders = true;
                let mut has_placeholder = false;
                while j < len && bytes[j] != b')' {
                    match bytes[j] {
                        b'$' => {
                            has_placeholder = true;
                            j += 1;
                            if j < len && (bytes[j] == b'N' || bytes[j] == b'S') {
                                j += 1;
                            } else {
                                all_placeholders = false;
                                break;
                            }
                        }
                        b',' | b' ' => j += 1,
                        _ => {
                            all_placeholders = false;
                            break;
                        }
                    }
                }
                if j < len && bytes[j] == b')' && all_placeholders && has_placeholder {
                    result.push_str(&sql[i..i + 2]); // "IN" or "in"
                    result.push_str(" ($...)");
                    i = j + 1;
                    continue;
                }
            }
        }
        result.push(bytes[i] as char);
        i += 1;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_literals() {
        assert_eq!(
            fingerprint("SELECT * FROM users WHERE name = 'alice'"),
            "select * from users where name = $s"
        );
    }

    #[test]
    fn test_numeric_literals() {
        assert_eq!(
            fingerprint("SELECT * FROM users WHERE id = 42"),
            "select * from users where id = $n"
        );
    }

    #[test]
    fn test_mixed() {
        assert_eq!(
            fingerprint("UPDATE orders SET status = 'shipped' WHERE id = 123 AND price > 9.99"),
            "update orders set status = $s where id = $n and price > $n"
        );
    }

    #[test]
    fn test_in_list() {
        assert_eq!(
            fingerprint("SELECT * FROM t WHERE id IN (1, 2, 3)"),
            "select * from t where id in ($...)"
        );
    }

    #[test]
    fn test_table_names_preserved() {
        assert_eq!(
            fingerprint("SELECT * FROM table1 WHERE col2 = 5"),
            "select * from table1 where col2 = $n"
        );
    }

    #[test]
    fn test_escaped_quotes() {
        assert_eq!(
            fingerprint("SELECT * FROM t WHERE name = 'it''s'"),
            "select * from t where name = $s"
        );
    }
}
