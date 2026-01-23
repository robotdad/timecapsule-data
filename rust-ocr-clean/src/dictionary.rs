//! Multi-language dictionary lookup for OCR vocab validation.
//!
//! Loads Hunspell dictionaries for English, German, French, and Latin
//! to validate words during vocab extraction. Words matching ANY language
//! are considered valid (not suspicious).

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::OnceLock;
use zspell::Dictionary;

/// Global dictionary instances (loaded once, reused)
static DICTIONARIES: OnceLock<MultiLangDict> = OnceLock::new();

/// Multi-language dictionary container
pub struct MultiLangDict {
    english: Option<Dictionary>,
    german: Option<Dictionary>,
    french: Option<Dictionary>,
    latin: HashSet<String>,  // Simple word list (Hunspell format incompatible with zspell)
}

impl MultiLangDict {
    /// Load dictionaries from the given directory
    pub fn load(dict_dir: &Path) -> Self {
        Self {
            english: load_dict(dict_dir, "en_US"),
            german: load_dict(dict_dir, "de_DE"),
            french: load_dict(dict_dir, "fr_FR"),
            latin: load_latin_wordlist(dict_dir),
        }
    }

    /// Check if a word exists in ANY loaded dictionary
    pub fn check(&self, word: &str) -> bool {
        // Try exact match first
        if self.check_exact(word) {
            return true;
        }
        // Try lowercase
        let lower = word.to_lowercase();
        if lower != word && self.check_exact(&lower) {
            return true;
        }
        false
    }

    fn check_exact(&self, word: &str) -> bool {
        if let Some(ref d) = self.english {
            if d.check_word(word) {
                return true;
            }
        }
        if let Some(ref d) = self.german {
            if d.check_word(word) {
                return true;
            }
        }
        if let Some(ref d) = self.french {
            if d.check_word(word) {
                return true;
            }
        }
        if self.latin.contains(word) {
            return true;
        }
        false
    }

    /// Check which language(s) a word belongs to (for debugging)
    pub fn check_languages(&self, word: &str) -> Vec<&'static str> {
        let mut langs = Vec::new();
        let lower = word.to_lowercase();

        if let Some(ref d) = self.english {
            if d.check_word(word) || d.check_word(&lower) {
                langs.push("en");
            }
        }
        if let Some(ref d) = self.german {
            if d.check_word(word) || d.check_word(&lower) {
                langs.push("de");
            }
        }
        if let Some(ref d) = self.french {
            if d.check_word(word) || d.check_word(&lower) {
                langs.push("fr");
            }
        }
        if self.latin.contains(word) || self.latin.contains(&lower) {
            langs.push("la");
        }
        langs
    }

    /// Get stats about loaded dictionaries
    pub fn stats(&self) -> String {
        format!(
            "Dictionaries loaded: en={}, de={}, fr={}, la={}",
            self.english.is_some(),
            self.german.is_some(),
            self.french.is_some(),
            !self.latin.is_empty()
        )
    }
}

/// Load Latin as a simple word list (Hunspell format incompatible with zspell)
fn load_latin_wordlist(dict_dir: &Path) -> HashSet<String> {
    let wordlist_path = dict_dir.join("la_words.txt");
    
    if !wordlist_path.exists() {
        eprintln!("Latin word list not found: la_words.txt");
        return HashSet::new();
    }
    
    match fs::read_to_string(&wordlist_path) {
        Ok(content) => {
            let words: HashSet<String> = content
                .lines()
                .filter(|line| !line.is_empty() && !line.starts_with('#'))
                .map(|line| line.to_string())
                .collect();
            eprintln!("Loaded Latin word list: {} words", words.len());
            words
        }
        Err(e) => {
            eprintln!("Failed to read la_words.txt: {}", e);
            HashSet::new()
        }
    }
}

/// Load a single Hunspell dictionary using zspell builder pattern
fn load_dict(dict_dir: &Path, name: &str) -> Option<Dictionary> {
    let aff_path = dict_dir.join(format!("{}.aff", name));
    let dic_path = dict_dir.join(format!("{}.dic", name));

    if !aff_path.exists() || !dic_path.exists() {
        eprintln!("Dictionary not found: {}", name);
        return None;
    }

    // Read file contents
    let aff_content = match fs::read_to_string(&aff_path) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Failed to read {}.aff: {}", name, e);
            return None;
        }
    };

    let dic_content = match fs::read_to_string(&dic_path) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Failed to read {}.dic: {}", name, e);
            return None;
        }
    };

    // Use zspell builder pattern
    match zspell::builder()
        .config_str(&aff_content)
        .dict_str(&dic_content)
        .build()
    {
        Ok(dict) => {
            eprintln!("Loaded dictionary: {}", name);
            Some(dict)
        }
        Err(e) => {
            eprintln!("Failed to build dictionary {}: {}", name, e);
            None
        }
    }
}

/// Initialize dictionaries from a directory path
pub fn init_dictionaries(dict_dir: &str) -> bool {
    let path = Path::new(dict_dir);
    if !path.exists() {
        eprintln!("Dictionary directory not found: {}", dict_dir);
        return false;
    }

    let dict = MultiLangDict::load(path);
    let stats = dict.stats();
    
    match DICTIONARIES.set(dict) {
        Ok(_) => {
            eprintln!("Dictionary initialization complete: {}", stats);
            true
        }
        Err(_) => {
            eprintln!("Dictionaries already initialized");
            true
        }
    }
}

/// Check if a word is valid in any loaded dictionary
pub fn is_known_word(word: &str) -> bool {
    match DICTIONARIES.get() {
        Some(dict) => dict.check(word),
        None => false, // Dictionaries not loaded
    }
}

/// Check which languages recognize a word (for debugging)
pub fn word_languages(word: &str) -> Vec<&'static str> {
    match DICTIONARIES.get() {
        Some(dict) => dict.check_languages(word),
        None => Vec::new(),
    }
}

/// Check if dictionaries are loaded
pub fn dictionaries_loaded() -> bool {
    DICTIONARIES.get().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dictionary_loading() {
        // This test requires dictionaries to be present
        let dict_dir = Path::new("dictionaries");
        if dict_dir.exists() {
            let dict = MultiLangDict::load(dict_dir);
            
            // Test English
            assert!(dict.check("hello"));
            assert!(dict.check("Still"));
            assert!(dict.check("William"));
            
            // Test non-words
            assert!(!dict.check("asdfgh"));
            assert!(!dict.check("xyzqwerty"));
        }
    }
}
