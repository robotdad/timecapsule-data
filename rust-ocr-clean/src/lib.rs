use pyo3::prelude::*;
use regex::Regex;
use lazy_static::lazy_static;
use unicode_normalization::UnicodeNormalization;
use whatlang::{detect, Lang};
use rayon::prelude::*;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

mod dictionary;

// Pre-compile all OCR patterns at module load time
// Each pattern is: (regex, replacement, optional_context_regex, category)
lazy_static! {
    static ref OCR_PATTERNS: Vec<(Regex, &'static str, Option<Regex>, &'static str)> = {
        vec![
            // 'the' variants (most common) - li/h confusion
            (Regex::new(r"(?i)\btbe\b").unwrap(), "the", None, "li_h_confusion"),
            (Regex::new(r"(?i)\btlie\b").unwrap(), "the", None, "li_h_confusion"),
            (Regex::new(r"(?i)\btiie\b").unwrap(), "the", None, "li_h_confusion"),
            (Regex::new(r"(?i)\btbc\b").unwrap(), "the", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bihe\b").unwrap(), "the", None, "li_h_confusion"),
            (Regex::new(r"(?i)\btne\b").unwrap(), "the", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bthc\b").unwrap(), "the", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bllie\b").unwrap(), "the", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bllic\b").unwrap(), "the", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bllio\b").unwrap(), "the", None, "li_h_confusion"),
            
            // 'this' variants
            (Regex::new(r"(?i)\btbis\b").unwrap(), "this", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bthia\b").unwrap(), "this", None, "li_h_confusion"),
            (Regex::new(r"(?i)\btliis\b").unwrap(), "this", None, "li_h_confusion"),
            
            // 'that' variants
            (Regex::new(r"(?i)\btbat\b").unwrap(), "that", None, "li_h_confusion"),
            (Regex::new(r"(?i)\btliat\b").unwrap(), "that", None, "li_h_confusion"),
            (Regex::new(r"(?i)\btlmt\b").unwrap(), "that", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bthnt\b").unwrap(), "that", None, "li_h_confusion"),
            
            // 'which' variants
            (Regex::new(r"(?i)\bwbich\b").unwrap(), "which", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bwhicb\b").unwrap(), "which", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bwliich\b").unwrap(), "which", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bwliicli\b").unwrap(), "which", None, "li_h_confusion"),
            
            // 'what' variants
            (Regex::new(r"(?i)\bwliat\b").unwrap(), "what", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bwlmt\b").unwrap(), "what", None, "li_h_confusion"),
            
            // 'when' variants
            (Regex::new(r"(?i)\bwlien\b").unwrap(), "when", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bwben\b").unwrap(), "when", None, "li_h_confusion"),
            
            // 'where' variants
            (Regex::new(r"(?i)\bwliere\b").unwrap(), "where", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bwbere\b").unwrap(), "where", None, "li_h_confusion"),
            
            // 'while' variants
            (Regex::new(r"(?i)\bwliile\b").unwrap(), "while", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bwbile\b").unwrap(), "while", None, "li_h_confusion"),
            
            // 'who' variants
            (Regex::new(r"(?i)\bwlio\b").unwrap(), "who", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bwliose\b").unwrap(), "whose", None, "li_h_confusion"),
            
            // 'him' variants
            (Regex::new(r"(?i)\bliim\b").unwrap(), "him", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bhirn\b").unwrap(), "him", None, "li_h_confusion"),
            
            // 'his' variants
            (Regex::new(r"(?i)\bliis\b").unwrap(), "his", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bhia\b").unwrap(), "his", None, "li_h_confusion"),
            
            // 'her' variants
            (Regex::new(r"(?i)\blier\b").unwrap(), "her", None, "li_h_confusion"),
            
            // 'he' - needs context
            (
                Regex::new(r"(?i)\blie\b").unwrap(),
                "he",
                Some(Regex::new(r"(?i)\b(and|but|that|when|if|as|so|because)\s+lie\b").unwrap()),
                "li_h_confusion"
            ),
            
            // 'she' variants
            (Regex::new(r"(?i)\bslie\b").unwrap(), "she", None, "li_h_confusion"),
            
            // 'they' variants
            (Regex::new(r"(?i)\btliey\b").unwrap(), "they", None, "li_h_confusion"),
            (Regex::new(r"(?i)\btbey\b").unwrap(), "they", None, "li_h_confusion"),
            
            // 'their' variants
            (Regex::new(r"(?i)\btbeir\b").unwrap(), "their", None, "li_h_confusion"),
            (Regex::new(r"(?i)\btlieir\b").unwrap(), "their", None, "li_h_confusion"),
            
            // 'them' variants
            (Regex::new(r"(?i)\btbem\b").unwrap(), "them", None, "li_h_confusion"),
            (Regex::new(r"(?i)\btliem\b").unwrap(), "them", None, "li_h_confusion"),
            
            // 'then' variants
            (Regex::new(r"(?i)\btben\b").unwrap(), "then", None, "li_h_confusion"),
            (Regex::new(r"(?i)\btlien\b").unwrap(), "then", None, "li_h_confusion"),
            
            // 'there' variants
            (Regex::new(r"(?i)\btbere\b").unwrap(), "there", None, "li_h_confusion"),
            (Regex::new(r"(?i)\btliere\b").unwrap(), "there", None, "li_h_confusion"),
            
            // 'these' variants
            (Regex::new(r"(?i)\btbese\b").unwrap(), "these", None, "li_h_confusion"),
            (Regex::new(r"(?i)\btliese\b").unwrap(), "these", None, "li_h_confusion"),
            
            // 'those' variants
            (Regex::new(r"(?i)\btbose\b").unwrap(), "those", None, "li_h_confusion"),
            (Regex::new(r"(?i)\btliose\b").unwrap(), "those", None, "li_h_confusion"),
            
            // 'other' variants
            (Regex::new(r"(?i)\botber\b").unwrap(), "other", None, "li_h_confusion"),
            (Regex::new(r"(?i)\botlier\b").unwrap(), "other", None, "li_h_confusion"),
            
            // 'and' variants
            (Regex::new(r"(?i)\barid\b").unwrap(), "and", None, "li_h_confusion"),
            (Regex::new(r"(?i)\baud\b").unwrap(), "and", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bnnd\b").unwrap(), "and", None, "li_h_confusion"),
            (Regex::new(r"(?i)\baiid\b").unwrap(), "and", None, "li_h_confusion"),
            
            // 'with' variants
            (Regex::new(r"(?i)\bwitb\b").unwrap(), "with", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bwitli\b").unwrap(), "with", None, "li_h_confusion"),
            
            // 'have' variants
            (Regex::new(r"(?i)\bhavo\b").unwrap(), "have", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bbave\b").unwrap(), "have", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bliave\b").unwrap(), "have", None, "li_h_confusion"),
            
            // Other common variants
            (Regex::new(r"(?i)\bboen\b").unwrap(), "been", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfrorn\b").unwrap(), "from", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bwero\b").unwrap(), "were", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bwonld\b").unwrap(), "would", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bwouid\b").unwrap(), "would", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bconld\b").unwrap(), "could", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bcouid\b").unwrap(), "could", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bsbould\b").unwrap(), "should", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bshouid\b").unwrap(), "should", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bbeiug\b").unwrap(), "being", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bmado\b").unwrap(), "made", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bnpon\b").unwrap(), "upon", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bsucb\b").unwrap(), "such", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bsucli\b").unwrap(), "such", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bsomo\b").unwrap(), "some", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bverv\b").unwrap(), "very", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bllrst\b").unwrap(), "first", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfirst\b").unwrap(), "first", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bftill\b").unwrap(), "still", None, "li_h_confusion"),
            
            // Long s (ſ -> s) - VERY common in old texts
            (Regex::new(r"ſ").unwrap(), "s", None, "li_h_confusion"),
            
            // Long-s OCR artifacts (ſ misread as f)
            (Regex::new(r"(?i)\bfuch\b").unwrap(), "such", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfome\b").unwrap(), "some", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfame\b").unwrap(), "same", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfaid\b").unwrap(), "said", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfays\b").unwrap(), "says", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfay\b").unwrap(), "say", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfaw\b").unwrap(), "saw", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfee\b").unwrap(), "see", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfeen\b").unwrap(), "seen", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfeems\b").unwrap(), "seems", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfeem\b").unwrap(), "seem", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfelf\b").unwrap(), "self", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfent\b").unwrap(), "sent", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfet\b").unwrap(), "set", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfhall\b").unwrap(), "shall", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfhould\b").unwrap(), "should", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfhe\b").unwrap(), "she", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfide\b").unwrap(), "side", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfince\b").unwrap(), "since", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfir\b").unwrap(), "sir", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfmall\b").unwrap(), "small", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfo\b").unwrap(), "so", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfon\b").unwrap(), "son", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfoon\b").unwrap(), "soon", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfoul\b").unwrap(), "soul", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfpeak\b").unwrap(), "speak", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfpoke\b").unwrap(), "spoke", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bftand\b").unwrap(), "stand", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bftate\b").unwrap(), "state", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bftates\b").unwrap(), "states", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bftill\b").unwrap(), "still", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bftood\b").unwrap(), "stood", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bftrong\b").unwrap(), "strong", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfubject\b").unwrap(), "subject", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfuffer\b").unwrap(), "suffer", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfupport\b").unwrap(), "support", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfure\b").unwrap(), "sure", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bfyftem\b").unwrap(), "system", None, "li_h_confusion"),
            
            // rn/m confusion
            (Regex::new(r"(?i)\brnay\b").unwrap(), "may", None, "li_h_confusion"),
            (Regex::new(r"(?i)\brnuch\b").unwrap(), "much", None, "li_h_confusion"),
            (Regex::new(r"(?i)\brnore\b").unwrap(), "more", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bsarne\b").unwrap(), "same", None, "li_h_confusion"),
            (Regex::new(r"(?i)\btirne\b").unwrap(), "time", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bnarne\b").unwrap(), "name", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bcorne\b").unwrap(), "come", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bhorne\b").unwrap(), "home", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bconntry\b").unwrap(), "country", None, "li_h_confusion"),
            (Regex::new(r"(?i)\bhnndred\b").unwrap(), "hundred", None, "li_h_confusion"),
            
            // ll -> U confusion (VERY common in this corpus)
            (Regex::new(r"(?i)\bwiU\b").unwrap(), "will", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bweU\b").unwrap(), "well", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfuU\b").unwrap(), "full", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bsmaU\b").unwrap(), "small", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bstiU\b").unwrap(), "still", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bcaUed\b").unwrap(), "called", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bcaUing\b").unwrap(), "calling", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfoUow\b").unwrap(), "follow", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfoUows\b").unwrap(), "follows", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfoUowing\b").unwrap(), "following", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfoUowed\b").unwrap(), "followed", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bshaU\b").unwrap(), "shall", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfeU\b").unwrap(), "fell", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bteU\b").unwrap(), "tell", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bseU\b").unwrap(), "sell", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfiU\b").unwrap(), "fill", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bkiU\b").unwrap(), "kill", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bskiU\b").unwrap(), "skill", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bmiU\b").unwrap(), "mill", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bbiU\b").unwrap(), "bill", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bhiU\b").unwrap(), "hill", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btiU\b").unwrap(), "till", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bpuU\b").unwrap(), "pull", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\baU\b").unwrap(), "all", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bbaU\b").unwrap(), "ball", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bwaU\b").unwrap(), "wall", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfaU\b").unwrap(), "fall", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bcaU\b").unwrap(), "call", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btaU\b").unwrap(), "tall", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bdoUars\b").unwrap(), "dollars", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bcoUege\b").unwrap(), "college", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bcoUection\b").unwrap(), "collection", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bexceUent\b").unwrap(), "excellent", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\binteUigent\b").unwrap(), "intelligent", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\binteUigence\b").unwrap(), "intelligence", None, "ll_u_confusion"),
            // More ll -> U common words
            (Regex::new(r"(?i)\bpubUc\b").unwrap(), "public", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bengUsh\b").unwrap(), "English", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bheaUh\b").unwrap(), "health", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\blitUe\b").unwrap(), "little", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfuUy\b").unwrap(), "fully", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfeUow\b").unwrap(), "fellow", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bparUament\b").unwrap(), "parliament", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bmiUtary\b").unwrap(), "military", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bmUe\b").unwrap(), "mile", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bmUes\b").unwrap(), "miles", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bpoUcy\b").unwrap(), "policy", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bappUed\b").unwrap(), "applied", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bappUy\b").unwrap(), "apply", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bappUcation\b").unwrap(), "application", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bappUcations\b").unwrap(), "applications", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bestabUshed\b").unwrap(), "established", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bgenUeman\b").unwrap(), "gentleman", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bgeneraUy\b").unwrap(), "generally", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\baUowed\b").unwrap(), "allowed", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\baUow\b").unwrap(), "allow", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bviUage\b").unwrap(), "village", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bviUages\b").unwrap(), "villages", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bwhoUy\b").unwrap(), "wholly", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bbuUt\b").unwrap(), "built", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\byeUow\b").unwrap(), "yellow", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bcoUonel\b").unwrap(), "colonel", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bbeUeved\b").unwrap(), "believed", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bbeUeve\b").unwrap(), "believe", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bbeUef\b").unwrap(), "belief", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bmiUions\b").unwrap(), "millions", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bmiUion\b").unwrap(), "million", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bdaUy\b").unwrap(), "daily", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bdeUvered\b").unwrap(), "delivered", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bdeUver\b").unwrap(), "deliver", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bvaUey\b").unwrap(), "valley", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bvaUeys\b").unwrap(), "valleys", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bkiUed\b").unwrap(), "killed", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bespeciaUy\b").unwrap(), "especially", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bchUdren\b").unwrap(), "children", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfeeUng\b").unwrap(), "feeling", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfeeUngs\b").unwrap(), "feelings", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfamUy\b").unwrap(), "family", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfamUies\b").unwrap(), "families", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bhoUow\b").unwrap(), "hollow", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfaUen\b").unwrap(), "fallen", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfaUing\b").unwrap(), "falling", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bpoUtics\b").unwrap(), "politics", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bpoUtical\b").unwrap(), "political", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\brebeUion\b").unwrap(), "rebellion", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\baUies\b").unwrap(), "allies", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\baUied\b").unwrap(), "allied", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bequaUy\b").unwrap(), "equally", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\busuaUy\b").unwrap(), "usually", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bquaUty\b").unwrap(), "quality", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bcoUected\b").unwrap(), "collected", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bcoUect\b").unwrap(), "collect", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\braUroad\b").unwrap(), "railroad", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\boriginaUy\b").unwrap(), "originally", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bbrUiant\b").unwrap(), "brilliant", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\brepubUc\b").unwrap(), "republic", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bcathoUc\b").unwrap(), "catholic", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bchanceUor\b").unwrap(), "chancellor", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bprobaUy\b").unwrap(), "probably", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bbuUding\b").unwrap(), "building", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bbuUdings\b").unwrap(), "buildings", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bentiUed\b").unwrap(), "entitled", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bwooUen\b").unwrap(), "woollen", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bmetropoUtan\b").unwrap(), "metropolitan", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bitaUan\b").unwrap(), "Italian", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\biUustrated\b").unwrap(), "illustrated", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\biUustration\b").unwrap(), "illustration", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bveUum\b").unwrap(), "vellum", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bfoUo\b").unwrap(), "folio", None, "ll_u_confusion"),
            // Proper names with ll -> U
            (Regex::new(r"\bWiUiam\b").unwrap(), "William", None, "ll_u_confusion"),
            (Regex::new(r"\bWilUam\b").unwrap(), "William", None, "ll_u_confusion"),
            (Regex::new(r"\bWiUiams\b").unwrap(), "Williams", None, "ll_u_confusion"),
            (Regex::new(r"\bPhiUip\b").unwrap(), "Philip", None, "ll_u_confusion"),
            (Regex::new(r"\bPhiUppine\b").unwrap(), "Philippine", None, "ll_u_confusion"),
            (Regex::new(r"\bPhiUppines\b").unwrap(), "Philippines", None, "ll_u_confusion"),
            (Regex::new(r"\bDubUn\b").unwrap(), "Dublin", None, "ll_u_confusion"),
            (Regex::new(r"\bBerUn\b").unwrap(), "Berlin", None, "ll_u_confusion"),
            (Regex::new(r"\bApoUo\b").unwrap(), "Apollo", None, "ll_u_confusion"),
            (Regex::new(r"\blUinois\b").unwrap(), "Illinois", None, "ll_u_confusion"),
            (Regex::new(r"\bCaroUna\b").unwrap(), "Carolina", None, "ll_u_confusion"),
            (Regex::new(r"\bNashviUe\b").unwrap(), "Nashville", None, "ll_u_confusion"),
            (Regex::new(r"\bHoUand\b").unwrap(), "Holland", None, "ll_u_confusion"),
            (Regex::new(r"\bViUa\b").unwrap(), "Villa", None, "ll_u_confusion"),
            
            // Additional h/li errors from corpus analysis
            (Regex::new(r"(?i)\btke\b").unwrap(), "the", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bsnch\b").unwrap(), "such", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bmnch\b").unwrap(), "much", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bmnst\b").unwrap(), "must", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bthns\b").unwrap(), "thus", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bwonld\b").unwrap(), "would", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bconld\b").unwrap(), "could", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bsliould\b").unwrap(), "should", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bliave\b").unwrap(), "have", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bliaving\b").unwrap(), "having", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bliead\b").unwrap(), "head", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\blieart\b").unwrap(), "heart", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bliand\b").unwrap(), "hand", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bliouse\b").unwrap(), "house", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bliow\b").unwrap(), "how", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bliope\b").unwrap(), "hope", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bliere\b").unwrap(), "here", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bliigh\b").unwrap(), "high", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bliistory\b").unwrap(), "history", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\blialf\b").unwrap(), "half", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bliold\b").unwrap(), "hold", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\blioly\b").unwrap(), "holy", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\blionor\b").unwrap(), "honor", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\blionour\b").unwrap(), "honour", None, "ll_u_confusion"),
            // More li/h errors (tli->th, lli->ll patterns)
            (Regex::new(r"(?i)\btliis\b").unwrap(), "this", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btliia\b").unwrap(), "this", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btliat\b").unwrap(), "that", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btlie\b").unwrap(), "the", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btlien\b").unwrap(), "then", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btliere\b").unwrap(), "there", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btliey\b").unwrap(), "they", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btliem\b").unwrap(), "them", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btlieir\b").unwrap(), "their", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btliese\b").unwrap(), "these", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btliose\b").unwrap(), "those", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btliough\b").unwrap(), "though", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btlirough\b").unwrap(), "through", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btliink\b").unwrap(), "think", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btliings\b").unwrap(), "things", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\btliing\b").unwrap(), "thing", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bwliich\b").unwrap(), "which", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bwliile\b").unwrap(), "while", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bwlien\b").unwrap(), "when", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bwliat\b").unwrap(), "what", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bwliere\b").unwrap(), "where", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bwliether\b").unwrap(), "whether", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bwliole\b").unwrap(), "whole", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bwliom\b").unwrap(), "whom", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bwliose\b").unwrap(), "whose", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bcliild\b").unwrap(), "child", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bcliildren\b").unwrap(), "children", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bcliief\b").unwrap(), "chief", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bcliurch\b").unwrap(), "church", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bnotliing\b").unwrap(), "nothing", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bsometliing\b").unwrap(), "something", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\beverytliing\b").unwrap(), "everything", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\banytliing\b").unwrap(), "anything", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bliigli\b").unwrap(), "high", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bliigh\b").unwrap(), "high", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bliiglier\b").unwrap(), "higher", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\bliigliest\b").unwrap(), "highest", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\blliey\b").unwrap(), "they", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\blliere\b").unwrap(), "there", None, "ll_u_confusion"),
            (Regex::new(r"(?i)\blliat\b").unwrap(), "that", None, "ll_u_confusion"),
            
            // Common long-s words (very high frequency)
            (Regex::new(r"(?i)\balfo\b").unwrap(), "also", None, "long_s"),
            (Regex::new(r"(?i)\bmoft\b").unwrap(), "most", None, "long_s"),
            (Regex::new(r"(?i)\balmoft\b").unwrap(), "almost", None, "long_s"),
            (Regex::new(r"(?i)\bfhall\b").unwrap(), "shall", None, "long_s"),
            (Regex::new(r"(?i)\bfhould\b").unwrap(), "should", None, "long_s"),
            (Regex::new(r"(?i)\bmuft\b").unwrap(), "must", None, "long_s"),
            (Regex::new(r"(?i)\bjuft\b").unwrap(), "just", None, "long_s"),
            (Regex::new(r"(?i)\bfirft\b").unwrap(), "first", None, "long_s"),
            (Regex::new(r"(?i)\blaft\b").unwrap(), "last", None, "long_s"),
            (Regex::new(r"(?i)\bfaid\b").unwrap(), "said", None, "long_s"),
            (Regex::new(r"(?i)\bfame\b").unwrap(), "same", None, "long_s"),
            (Regex::new(r"(?i)\bfeen\b").unwrap(), "seen", None, "long_s"),
            (Regex::new(r"(?i)\bfeems\b").unwrap(), "seems", None, "long_s"),
            (Regex::new(r"(?i)\bfeem\b").unwrap(), "seem", None, "long_s"),
            (Regex::new(r"(?i)\bfent\b").unwrap(), "sent", None, "long_s"),
            (Regex::new(r"(?i)\bfet\b").unwrap(), "set", None, "long_s"),
            (Regex::new(r"(?i)\bfide\b").unwrap(), "side", None, "long_s"),
            (Regex::new(r"(?i)\bfince\b").unwrap(), "since", None, "long_s"),
            (Regex::new(r"(?i)\bfon\b").unwrap(), "son", None, "long_s"),
            (Regex::new(r"(?i)\bfoon\b").unwrap(), "soon", None, "long_s"),
            (Regex::new(r"(?i)\bftate\b").unwrap(), "state", None, "long_s"),
            (Regex::new(r"(?i)\bftates\b").unwrap(), "states", None, "long_s"),
            (Regex::new(r"(?i)\bftill\b").unwrap(), "still", None, "long_s"),
            (Regex::new(r"(?i)\bftrong\b").unwrap(), "strong", None, "long_s"),
            (Regex::new(r"(?i)\bfubject\b").unwrap(), "subject", None, "long_s"),
            (Regex::new(r"(?i)\bfyftem\b").unwrap(), "system", None, "long_s"),
            (Regex::new(r"(?i)\bthefe\b").unwrap(), "these", None, "long_s"),
            (Regex::new(r"(?i)\bthofe\b").unwrap(), "those", None, "long_s"),
            (Regex::new(r"(?i)\bufe\b").unwrap(), "use", None, "long_s"),
            (Regex::new(r"(?i)\bufed\b").unwrap(), "used", None, "long_s"),
            (Regex::new(r"(?i)\bwife\b").unwrap(), "wise", None, "long_s"),
            (Regex::new(r"(?i)\bwifdom\b").unwrap(), "wisdom", None, "long_s"),
            (Regex::new(r"(?i)\byourfelf\b").unwrap(), "yourself", None, "long_s"),
            
            // More long-s artifacts
            (Regex::new(r"(?i)\bhimfelf\b").unwrap(), "himself", None, "long_s"),
            (Regex::new(r"(?i)\bherfelf\b").unwrap(), "herself", None, "long_s"),
            (Regex::new(r"(?i)\bitfelf\b").unwrap(), "itself", None, "long_s"),
            (Regex::new(r"(?i)\bmyfelf\b").unwrap(), "myself", None, "long_s"),
            (Regex::new(r"(?i)\byourfelf\b").unwrap(), "yourself", None, "long_s"),
            (Regex::new(r"(?i)\bthemfelves\b").unwrap(), "themselves", None, "long_s"),
            (Regex::new(r"(?i)\bourfelves\b").unwrap(), "ourselves", None, "long_s"),
            (Regex::new(r"(?i)\bfufficient\b").unwrap(), "sufficient", None, "long_s"),
            (Regex::new(r"(?i)\bfuflScient\b").unwrap(), "sufficient", None, "long_s"),
            (Regex::new(r"(?i)\bfuccefsful\b").unwrap(), "successful", None, "long_s"),
            (Regex::new(r"(?i)\bfuccefs\b").unwrap(), "success", None, "long_s"),
            (Regex::new(r"(?i)\bnecefsary\b").unwrap(), "necessary", None, "long_s"),
            (Regex::new(r"(?i)\bpoffible\b").unwrap(), "possible", None, "long_s"),
            (Regex::new(r"(?i)\bimpoffible\b").unwrap(), "impossible", None, "long_s"),
            (Regex::new(r"(?i)\bpoffefs\b").unwrap(), "possess", None, "long_s"),
            (Regex::new(r"(?i)\bpoffeffion\b").unwrap(), "possession", None, "long_s"),
            (Regex::new(r"(?i)\bpaffage\b").unwrap(), "passage", None, "long_s"),
            (Regex::new(r"(?i)\bpaffed\b").unwrap(), "passed", None, "long_s"),
            (Regex::new(r"(?i)\bpafs\b").unwrap(), "pass", None, "long_s"),
            (Regex::new(r"(?i)\bclafses\b").unwrap(), "classes", None, "long_s"),
            (Regex::new(r"(?i)\bclafs\b").unwrap(), "class", None, "long_s"),
            (Regex::new(r"(?i)\bmafs\b").unwrap(), "mass", None, "long_s"),
            (Regex::new(r"(?i)\blefs\b").unwrap(), "less", None, "long_s"),
            (Regex::new(r"(?i)\bunlefs\b").unwrap(), "unless", None, "long_s"),
            (Regex::new(r"(?i)\bbufinefs\b").unwrap(), "business", None, "long_s"),
            (Regex::new(r"(?i)\bcongrefs\b").unwrap(), "congress", None, "long_s"),
            (Regex::new(r"(?i)\bprogrefs\b").unwrap(), "progress", None, "long_s"),
            (Regex::new(r"(?i)\bexprefs\b").unwrap(), "express", None, "long_s"),
            (Regex::new(r"(?i)\bpoffefs\b").unwrap(), "possess", None, "long_s"),
            (Regex::new(r"(?i)\bwouM\b").unwrap(), "would", None, "long_s"),
            (Regex::new(r"(?i)\bcouM\b").unwrap(), "could", None, "long_s"),
            (Regex::new(r"(?i)\bshouM\b").unwrap(), "should", None, "long_s"),
            
            // More ll -> U variants (WlU, Wili patterns)
            (Regex::new(r"(?i)\bwlU\b").unwrap(), "will", None, "long_s"),
            (Regex::new(r"(?i)\bwili\b").unwrap(), "will", None, "long_s"),
            (Regex::new(r"(?i)\bwiili\b").unwrap(), "will", None, "long_s"),
            (Regex::new(r"(?i)\bstlU\b").unwrap(), "still", None, "long_s"),
            (Regex::new(r"(?i)\bstili\b").unwrap(), "still", None, "long_s"),
            (Regex::new(r"(?i)\bfuily\b").unwrap(), "fully", None, "long_s"),
            (Regex::new(r"(?i)\bfiily\b").unwrap(), "fully", None, "long_s"),
            (Regex::new(r"(?i)\breaily\b").unwrap(), "really", None, "long_s"),
            (Regex::new(r"(?i)\bfinaily\b").unwrap(), "finally", None, "long_s"),
            (Regex::new(r"(?i)\bspeciaily\b").unwrap(), "specially", None, "long_s"),
            (Regex::new(r"(?i)\bactuaily\b").unwrap(), "actually", None, "long_s"),
            (Regex::new(r"(?i)\bnaturaily\b").unwrap(), "naturally", None, "long_s"),
            
            // Additional long-s words (consolidated from Python)
            (Regex::new(r"(?i)\babfence\b").unwrap(), "absence", None, "long_s"),
            (Regex::new(r"(?i)\babfent\b").unwrap(), "absent", None, "long_s"),
            (Regex::new(r"(?i)\bbecaufe\b").unwrap(), "because", None, "long_s"),
            (Regex::new(r"(?i)\bbefides\b").unwrap(), "besides", None, "long_s"),
            (Regex::new(r"(?i)\bblindnefs\b").unwrap(), "blindness", None, "long_s"),
            (Regex::new(r"(?i)\bboldnefs\b").unwrap(), "boldness", None, "long_s"),
            (Regex::new(r"(?i)\bcaufe\b").unwrap(), "cause", None, "long_s"),
            (Regex::new(r"(?i)\bcaufed\b").unwrap(), "caused", None, "long_s"),
            (Regex::new(r"(?i)\bcaufes\b").unwrap(), "causes", None, "long_s"),
            (Regex::new(r"(?i)\bclofe\b").unwrap(), "close", None, "long_s"),
            (Regex::new(r"(?i)\bclofed\b").unwrap(), "closed", None, "long_s"),
            (Regex::new(r"(?i)\bclofely\b").unwrap(), "closely", None, "long_s"),
            (Regex::new(r"(?i)\bconfider\b").unwrap(), "consider", None, "long_s"),
            (Regex::new(r"(?i)\bconfidered\b").unwrap(), "considered", None, "long_s"),
            (Regex::new(r"(?i)\bcourfe\b").unwrap(), "course", None, "long_s"),
            (Regex::new(r"(?i)\bdarknefs\b").unwrap(), "darkness", None, "long_s"),
            (Regex::new(r"(?i)\bdefcribe\b").unwrap(), "describe", None, "long_s"),
            (Regex::new(r"(?i)\bdefcribed\b").unwrap(), "described", None, "long_s"),
            (Regex::new(r"(?i)\bdefign\b").unwrap(), "design", None, "long_s"),
            (Regex::new(r"(?i)\bdefigned\b").unwrap(), "designed", None, "long_s"),
            (Regex::new(r"(?i)\bdifcourfe\b").unwrap(), "discourse", None, "long_s"),
            (Regex::new(r"(?i)\bdoubtlefs\b").unwrap(), "doubtless", None, "long_s"),
            (Regex::new(r"(?i)\beafieft\b").unwrap(), "easiest", None, "long_s"),
            (Regex::new(r"(?i)\beafier\b").unwrap(), "easier", None, "long_s"),
            (Regex::new(r"(?i)\beafily\b").unwrap(), "easily", None, "long_s"),
            (Regex::new(r"(?i)\beafy\b").unwrap(), "easy", None, "long_s"),
            (Regex::new(r"(?i)\bfadnefs\b").unwrap(), "sadness", None, "long_s"),
            (Regex::new(r"(?i)\bfeafon\b").unwrap(), "season", None, "long_s"),
            (Regex::new(r"(?i)\bfeafons\b").unwrap(), "seasons", None, "long_s"),
            (Regex::new(r"(?i)\bficknefs\b").unwrap(), "sickness", None, "long_s"),
            (Regex::new(r"(?i)\bfuppofe\b").unwrap(), "suppose", None, "long_s"),
            (Regex::new(r"(?i)\bfuppofed\b").unwrap(), "supposed", None, "long_s"),
            (Regex::new(r"(?i)\bfuppofing\b").unwrap(), "supposing", None, "long_s"),
            (Regex::new(r"(?i)\bgoodnefs\b").unwrap(), "goodness", None, "long_s"),
            (Regex::new(r"(?i)\bgreatnefs\b").unwrap(), "greatness", None, "long_s"),
            (Regex::new(r"(?i)\bhappinefs\b").unwrap(), "happiness", None, "long_s"),
            (Regex::new(r"(?i)\bhoufe\b").unwrap(), "house", None, "long_s"),
            (Regex::new(r"(?i)\bhoufes\b").unwrap(), "houses", None, "long_s"),
            (Regex::new(r"(?i)\binfift\b").unwrap(), "insist", None, "long_s"),
            (Regex::new(r"(?i)\binfifted\b").unwrap(), "insisted", None, "long_s"),
            (Regex::new(r"(?i)\bkindnefs\b").unwrap(), "kindness", None, "long_s"),
            (Regex::new(r"(?i)\blikewife\b").unwrap(), "likewise", None, "long_s"),
            (Regex::new(r"(?i)\blofe\b").unwrap(), "lose", None, "long_s"),
            (Regex::new(r"(?i)\blofing\b").unwrap(), "losing", None, "long_s"),
            (Regex::new(r"(?i)\bloft\b").unwrap(), "lost", None, "long_s"),
            (Regex::new(r"(?i)\bmadnefs\b").unwrap(), "madness", None, "long_s"),
            (Regex::new(r"(?i)\bmeafure\b").unwrap(), "measure", None, "long_s"),
            (Regex::new(r"(?i)\bmeafured\b").unwrap(), "measured", None, "long_s"),
            (Regex::new(r"(?i)\bmeafures\b").unwrap(), "measures", None, "long_s"),
            (Regex::new(r"(?i)\bneceffity\b").unwrap(), "necessity", None, "long_s"),
            (Regex::new(r"(?i)\bobfervation\b").unwrap(), "observation", None, "long_s"),
            (Regex::new(r"(?i)\bobfervations\b").unwrap(), "observations", None, "long_s"),
            (Regex::new(r"(?i)\bobferve\b").unwrap(), "observe", None, "long_s"),
            (Regex::new(r"(?i)\bobferved\b").unwrap(), "observed", None, "long_s"),
            (Regex::new(r"(?i)\boccafion\b").unwrap(), "occasion", None, "long_s"),
            (Regex::new(r"(?i)\boccafioned\b").unwrap(), "occasioned", None, "long_s"),
            (Regex::new(r"(?i)\boccafions\b").unwrap(), "occasions", None, "long_s"),
            (Regex::new(r"(?i)\botherwife\b").unwrap(), "otherwise", None, "long_s"),
            (Regex::new(r"(?i)\bourfelf\b").unwrap(), "ourself", None, "long_s"),
            (Regex::new(r"(?i)\bpaffing\b").unwrap(), "passing", None, "long_s"),
            (Regex::new(r"(?i)\bpaft\b").unwrap(), "past", None, "long_s"),
            (Regex::new(r"(?i)\bperfon\b").unwrap(), "person", None, "long_s"),
            (Regex::new(r"(?i)\bperfons\b").unwrap(), "persons", None, "long_s"),
            (Regex::new(r"(?i)\bpleafe\b").unwrap(), "please", None, "long_s"),
            (Regex::new(r"(?i)\bpleafed\b").unwrap(), "pleased", None, "long_s"),
            (Regex::new(r"(?i)\bpleafure\b").unwrap(), "pleasure", None, "long_s"),
            (Regex::new(r"(?i)\bpoffeffed\b").unwrap(), "possessed", None, "long_s"),
            (Regex::new(r"(?i)\bpraife\b").unwrap(), "praise", None, "long_s"),
            (Regex::new(r"(?i)\bpraifed\b").unwrap(), "praised", None, "long_s"),
            (Regex::new(r"(?i)\bprefence\b").unwrap(), "presence", None, "long_s"),
            (Regex::new(r"(?i)\bprefent\b").unwrap(), "present", None, "long_s"),
            (Regex::new(r"(?i)\bprefented\b").unwrap(), "presented", None, "long_s"),
            (Regex::new(r"(?i)\bpreferve\b").unwrap(), "preserve", None, "long_s"),
            (Regex::new(r"(?i)\bpreferved\b").unwrap(), "preserved", None, "long_s"),
            (Regex::new(r"(?i)\bprofeffion\b").unwrap(), "profession", None, "long_s"),
            (Regex::new(r"(?i)\bprofeffor\b").unwrap(), "professor", None, "long_s"),
            (Regex::new(r"(?i)\bpurpofe\b").unwrap(), "purpose", None, "long_s"),
            (Regex::new(r"(?i)\bpurpofed\b").unwrap(), "purposed", None, "long_s"),
            (Regex::new(r"(?i)\bpurpofes\b").unwrap(), "purposes", None, "long_s"),
            (Regex::new(r"(?i)\braife\b").unwrap(), "raise", None, "long_s"),
            (Regex::new(r"(?i)\braifed\b").unwrap(), "raised", None, "long_s"),
            (Regex::new(r"(?i)\breafon\b").unwrap(), "reason", None, "long_s"),
            (Regex::new(r"(?i)\breafoned\b").unwrap(), "reasoned", None, "long_s"),
            (Regex::new(r"(?i)\breafons\b").unwrap(), "reasons", None, "long_s"),
            (Regex::new(r"(?i)\brefult\b").unwrap(), "result", None, "long_s"),
            (Regex::new(r"(?i)\brefults\b").unwrap(), "results", None, "long_s"),
            (Regex::new(r"(?i)\brehearfe\b").unwrap(), "rehearse", None, "long_s"),
            (Regex::new(r"(?i)\brehearfed\b").unwrap(), "rehearsed", None, "long_s"),
            (Regex::new(r"(?i)\breprefent\b").unwrap(), "represent", None, "long_s"),
            (Regex::new(r"(?i)\breprefented\b").unwrap(), "represented", None, "long_s"),
            (Regex::new(r"(?i)\breprefents\b").unwrap(), "represents", None, "long_s"),
            (Regex::new(r"(?i)\brife\b").unwrap(), "rise", None, "long_s"),
            (Regex::new(r"(?i)\brifen\b").unwrap(), "risen", None, "long_s"),
            (Regex::new(r"(?i)\brifing\b").unwrap(), "rising", None, "long_s"),
            (Regex::new(r"(?i)\brighteoufnefs\b").unwrap(), "righteousness", None, "long_s"),
            (Regex::new(r"(?i)\brofe\b").unwrap(), "rose", None, "long_s"),
            (Regex::new(r"(?i)\bthefe\b").unwrap(), "these", None, "long_s"),
            (Regex::new(r"(?i)\bthofe\b").unwrap(), "those", None, "long_s"),
            (Regex::new(r"(?i)\bthoufand\b").unwrap(), "thousand", None, "long_s"),
            (Regex::new(r"(?i)\bthoufands\b").unwrap(), "thousands", None, "long_s"),
            (Regex::new(r"(?i)\btreafure\b").unwrap(), "treasure", None, "long_s"),
            (Regex::new(r"(?i)\btreafures\b").unwrap(), "treasures", None, "long_s"),
            (Regex::new(r"(?i)\bufe\b").unwrap(), "use", None, "long_s"),
            (Regex::new(r"(?i)\bufed\b").unwrap(), "used", None, "long_s"),
            (Regex::new(r"(?i)\bufeful\b").unwrap(), "useful", None, "long_s"),
            (Regex::new(r"(?i)\bufelefs\b").unwrap(), "useless", None, "long_s"),
            (Regex::new(r"(?i)\bweaknefs\b").unwrap(), "weakness", None, "long_s"),
            (Regex::new(r"(?i)\bwhofe\b").unwrap(), "whose", None, "long_s"),
            (Regex::new(r"(?i)\bwickednefs\b").unwrap(), "wickedness", None, "long_s"),
            (Regex::new(r"(?i)\bwillingnefs\b").unwrap(), "willingness", None, "long_s"),
            // Short common long-s words
            (Regex::new(r"(?i)\bfo\b").unwrap(), "so", None, "long_s"),
            (Regex::new(r"(?i)\bfome\b").unwrap(), "some", None, "long_s"),
            (Regex::new(r"(?i)\bfoon\b").unwrap(), "soon", None, "long_s"),
            (Regex::new(r"(?i)\bfaid\b").unwrap(), "said", None, "long_s"),
            (Regex::new(r"(?i)\bfay\b").unwrap(), "say", None, "long_s"),
            (Regex::new(r"(?i)\bfays\b").unwrap(), "says", None, "long_s"),
            (Regex::new(r"(?i)\bfee\b").unwrap(), "see", None, "long_s"),
            (Regex::new(r"(?i)\bfeen\b").unwrap(), "seen", None, "long_s"),
            (Regex::new(r"(?i)\bfeem\b").unwrap(), "seem", None, "long_s"),
            (Regex::new(r"(?i)\bfeems\b").unwrap(), "seems", None, "long_s"),
            (Regex::new(r"(?i)\bfeemed\b").unwrap(), "seemed", None, "long_s"),
            (Regex::new(r"(?i)\bfent\b").unwrap(), "sent", None, "long_s"),
            (Regex::new(r"(?i)\bfet\b").unwrap(), "set", None, "long_s"),
            (Regex::new(r"(?i)\bfide\b").unwrap(), "side", None, "long_s"),
            (Regex::new(r"(?i)\bfides\b").unwrap(), "sides", None, "long_s"),
            (Regex::new(r"(?i)\bfince\b").unwrap(), "since", None, "long_s"),
            (Regex::new(r"(?i)\bfir\b").unwrap(), "sir", None, "long_s"),
            (Regex::new(r"(?i)\bfit\b").unwrap(), "sit", None, "long_s"),
            (Regex::new(r"(?i)\bfize\b").unwrap(), "size", None, "long_s"),
            (Regex::new(r"(?i)\bfon\b").unwrap(), "son", None, "long_s"),
            (Regex::new(r"(?i)\bfons\b").unwrap(), "sons", None, "long_s"),
            (Regex::new(r"(?i)\bfoul\b").unwrap(), "soul", None, "long_s"),
            (Regex::new(r"(?i)\bfouls\b").unwrap(), "souls", None, "long_s"),
            
            // ii/n confusion
            (Regex::new(r"(?i)\bkiiow\b").unwrap(), "know", None, "ii_n_confusion"),
            (Regex::new(r"(?i)\bkiiown\b").unwrap(), "known", None, "ii_n_confusion"),
            (Regex::new(r"(?i)\btiiis\b").unwrap(), "this", None, "ii_n_confusion"),
            (Regex::new(r"(?i)\bwiiich\b").unwrap(), "which", None, "ii_n_confusion"),
            (Regex::new(r"(?i)\bcliildren\b").unwrap(), "children", None, "ii_n_confusion"),
            
            // VV → W confusion (double-v misread as W or vice versa)
            (Regex::new(r"(?i)\bVVill\b").unwrap(), "Will", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVhy\b").unwrap(), "Why", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVest\b").unwrap(), "West", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVood\b").unwrap(), "Wood", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVashington\b").unwrap(), "Washington", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVhat\b").unwrap(), "What", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVhen\b").unwrap(), "When", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVhere\b").unwrap(), "Where", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVhich\b").unwrap(), "Which", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVith\b").unwrap(), "With", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVe\b").unwrap(), "We", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVas\b").unwrap(), "Was", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVere\b").unwrap(), "Were", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVork\b").unwrap(), "Work", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVorld\b").unwrap(), "World", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVar\b").unwrap(), "War", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVater\b").unwrap(), "Water", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVay\b").unwrap(), "Way", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVord\b").unwrap(), "Word", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVords\b").unwrap(), "Words", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVrite\b").unwrap(), "Write", None, "vv_w_confusion"),
            (Regex::new(r"(?i)\bVVritten\b").unwrap(), "Written", None, "vv_w_confusion"),
            
            // rn → m confusion (common OCR error)
            (Regex::new(r"(?i)\bFournal\b").unwrap(), "Journal", None, "rn_m_confusion"),
            (Regex::new(r"(?i)\bFrnm\b").unwrap(), "From", None, "rn_m_confusion"),
            (Regex::new(r"(?i)\bGoTernment\b").unwrap(), "Government", None, "rn_m_confusion"),
            (Regex::new(r"(?i)\bTrnth\b").unwrap(), "Truth", None, "rn_m_confusion"),
            (Regex::new(r"(?i)\bTrnst\b").unwrap(), "Trust", None, "rn_m_confusion"),
            (Regex::new(r"(?i)\bFornn\b").unwrap(), "Forum", None, "rn_m_confusion"),
            (Regex::new(r"(?i)\bTurnbuU\b").unwrap(), "Turnbull", None, "rn_m_confusion"),
            
            // U → ll confusion (uppercase I/l confusion in middle of words)
            (Regex::new(r"(?i)\bStUl\b").unwrap(), "Still", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bWUh\b").unwrap(), "With", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bWhUe\b").unwrap(), "While", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bCaUed\b").unwrap(), "Called", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bCeUs\b").unwrap(), "Cells", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bShaU\b").unwrap(), "Shall", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bBuU\b").unwrap(), "Bull", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bFuU\b").unwrap(), "Full", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bPuU\b").unwrap(), "Pull", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bWeU\b").unwrap(), "Well", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bTeU\b").unwrap(), "Tell", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bSeU\b").unwrap(), "Sell", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bFaU\b").unwrap(), "Fall", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bCaU\b").unwrap(), "Call", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bAU\b").unwrap(), "All", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bSmaU\b").unwrap(), "Small", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bWaU\b").unwrap(), "Wall", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bBaU\b").unwrap(), "Ball", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bHaU\b").unwrap(), "Hall", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bMiU\b").unwrap(), "Mill", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bHiU\b").unwrap(), "Hill", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bKiU\b").unwrap(), "Kill", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bBiU\b").unwrap(), "Bill", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bfiUed\b").unwrap(), "filled", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bkiUed\b").unwrap(), "killed", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bcaUed\b").unwrap(), "called", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bpuUed\b").unwrap(), "pulled", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bDifflcult\b").unwrap(), "Difficult", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bDifflculty\b").unwrap(), "Difficulty", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bDiflScult\b").unwrap(), "Difficult", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bDiflSculty\b").unwrap(), "Difficulty", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bDiflBcult\b").unwrap(), "Difficult", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bDiflBculty\b").unwrap(), "Difficulty", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bLiUle\b").unwrap(), "Little", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bOUier\b").unwrap(), "Other", None, "u_ll_confusion"),
            (Regex::new(r"(?i)\bPuUic\b").unwrap(), "Public", None, "u_ll_confusion"),
            
            // nn → m confusion (common in old OCR)
            (Regex::new(r"(?i)\bAnnnal\b").unwrap(), "Annual", None, "nn_m_confusion"),
            (Regex::new(r"(?i)\bConnnon\b").unwrap(), "Common", None, "nn_m_confusion"),
            (Regex::new(r"(?i)\bOonntry\b").unwrap(), "Country", None, "nn_m_confusion"),
            (Regex::new(r"(?i)\bAooonnt\b").unwrap(), "Account", None, "nn_m_confusion"),
            (Regex::new(r"(?i)\bFnnd\b").unwrap(), "Fund", None, "nn_m_confusion"),
            (Regex::new(r"(?i)\bFnnn\b").unwrap(), "Funn", None, "nn_m_confusion"),
            (Regex::new(r"(?i)\beonntry\b").unwrap(), "country", None, "nn_m_confusion"),
            
            // Ii → H confusion at word start (common OCR error)
            (Regex::new(r"\bIiis\b").unwrap(), "His", None, "ii_h_confusion"),
            (Regex::new(r"\bIiim\b").unwrap(), "Him", None, "ii_h_confusion"),
            (Regex::new(r"\bIiid\b").unwrap(), "Hid", None, "ii_h_confusion"),
            (Regex::new(r"\bIiit\b").unwrap(), "Hit", None, "ii_h_confusion"),
            (Regex::new(r"\bIiir\b").unwrap(), "Hir", None, "ii_h_confusion"),
            (Regex::new(r"\bIiad\b").unwrap(), "Had", None, "ii_h_confusion"),
            (Regex::new(r"\bIias\b").unwrap(), "Has", None, "ii_h_confusion"),
            (Regex::new(r"\bIiave\b").unwrap(), "Have", None, "ii_h_confusion"),
            (Regex::new(r"\bIiere\b").unwrap(), "Here", None, "ii_h_confusion"),
            (Regex::new(r"\bIiow\b").unwrap(), "How", None, "ii_h_confusion"),
            (Regex::new(r"\bIiouse\b").unwrap(), "House", None, "ii_h_confusion"),
            
            // Ull → lly patterns (broken ll ligature showing as U)
            (Regex::new(r"(?i)\bJoUy\b").unwrap(), "Jolly", None, "ull_lly"),
            (Regex::new(r"(?i)\bFoUy\b").unwrap(), "Folly", None, "ull_lly"),
            (Regex::new(r"(?i)\bEasUy\b").unwrap(), "Easily", None, "ull_lly"),
            (Regex::new(r"(?i)\bTuUy\b").unwrap(), "Fully", None, "ull_lly"),
            (Regex::new(r"(?i)\bFuUy\b").unwrap(), "Fully", None, "ull_lly"),
            (Regex::new(r"(?i)\bCarefuUy\b").unwrap(), "Carefully", None, "ull_lly"),
            (Regex::new(r"(?i)\bFinaUy\b").unwrap(), "Finally", None, "ull_lly"),
            (Regex::new(r"(?i)\bOccasionaUy\b").unwrap(), "Occasionally", None, "ull_lly"),
            (Regex::new(r"(?i)\bGraduaUy\b").unwrap(), "Gradually", None, "ull_lly"),
            (Regex::new(r"(?i)\bActuaUy\b").unwrap(), "Actually", None, "ull_lly"),
            (Regex::new(r"(?i)\bNaturaUy\b").unwrap(), "Naturally", None, "ull_lly"),
            (Regex::new(r"(?i)\bReadUy\b").unwrap(), "Readily", None, "ull_lly"),
            (Regex::new(r"(?i)\bUsuaUy\b").unwrap(), "Usually", None, "ull_lly"),
            (Regex::new(r"(?i)\bSpeciaUy\b").unwrap(), "Specially", None, "ull_lly"),
            (Regex::new(r"(?i)\bEspeciaUy\b").unwrap(), "Especially", None, "ull_lly"),
            (Regex::new(r"(?i)\bGeneraUy\b").unwrap(), "Generally", None, "ull_lly"),
            (Regex::new(r"(?i)\bPartiaUy\b").unwrap(), "Partially", None, "ull_lly"),
            (Regex::new(r"(?i)\bTotaUy\b").unwrap(), "Totally", None, "ull_lly"),
            (Regex::new(r"(?i)\bRealUy\b").unwrap(), "Really", None, "ull_lly"),
            (Regex::new(r"(?i)\bBeUy\b").unwrap(), "Belly", None, "ull_lly"),
            (Regex::new(r"(?i)\bKeUy\b").unwrap(), "Kelly", None, "ull_lly"),
            (Regex::new(r"(?i)\bHoUy\b").unwrap(), "Holly", None, "ull_lly"),
            (Regex::new(r"(?i)\bPoUy\b").unwrap(), "Polly", None, "ull_lly"),
            (Regex::new(r"(?i)\bDoUy\b").unwrap(), "Dolly", None, "ull_lly"),
            (Regex::new(r"(?i)\bMoUy\b").unwrap(), "Molly", None, "ull_lly"),
            (Regex::new(r"(?i)\bSaUy\b").unwrap(), "Sally", None, "ull_lly"),
            (Regex::new(r"(?i)\bVaUey\b").unwrap(), "Valley", None, "ull_lly"),
            (Regex::new(r"(?i)\bGaUery\b").unwrap(), "Gallery", None, "ull_lly"),
            (Regex::new(r"(?i)\bBuUet\b").unwrap(), "Bullet", None, "ull_lly"),
            (Regex::new(r"(?i)\bBuUetin\b").unwrap(), "Bulletin", None, "ull_lly"),
            (Regex::new(r"(?i)\bChieHy\b").unwrap(), "Chiefly", None, "ull_lly"),
            (Regex::new(r"(?i)\bBrieHy\b").unwrap(), "Briefly", None, "ull_lly"),
            
            // lf/fF → ff patterns (broken ff ligature)
            (Regex::new(r"(?i)\bDiflference\b").unwrap(), "Difference", None, "lf_ff"),
            (Regex::new(r"(?i)\bDiflferences\b").unwrap(), "Differences", None, "lf_ff"),
            (Regex::new(r"(?i)\bDiflferent\b").unwrap(), "Different", None, "lf_ff"),
            (Regex::new(r"(?i)\bDiflfers\b").unwrap(), "Differs", None, "lf_ff"),
            (Regex::new(r"(?i)\bDiflferential\b").unwrap(), "Differential", None, "lf_ff"),
            (Regex::new(r"(?i)\bOflFered\b").unwrap(), "Offered", None, "lf_ff"),
            (Regex::new(r"(?i)\bOflFer\b").unwrap(), "Offer", None, "lf_ff"),
            (Regex::new(r"(?i)\bOflFers\b").unwrap(), "Offers", None, "lf_ff"),
            (Regex::new(r"(?i)\bOflfence\b").unwrap(), "Offence", None, "lf_ff"),
            (Regex::new(r"(?i)\bOflfences\b").unwrap(), "Offences", None, "lf_ff"),
            (Regex::new(r"(?i)\bOflfensive\b").unwrap(), "Offensive", None, "lf_ff"),
            (Regex::new(r"(?i)\boflFense\b").unwrap(), "offense", None, "lf_ff"),
            (Regex::new(r"(?i)\bSuflfer\b").unwrap(), "Suffer", None, "lf_ff"),
            (Regex::new(r"(?i)\bSuflfers\b").unwrap(), "Suffers", None, "lf_ff"),
            (Regex::new(r"(?i)\bSuflfered\b").unwrap(), "Suffered", None, "lf_ff"),
            (Regex::new(r"(?i)\bSuflfering\b").unwrap(), "Suffering", None, "lf_ff"),
            (Regex::new(r"(?i)\bAflfections\b").unwrap(), "Affections", None, "lf_ff"),
            (Regex::new(r"(?i)\bAflfection\b").unwrap(), "Affection", None, "lf_ff"),
            (Regex::new(r"(?i)\baflForded\b").unwrap(), "afforded", None, "lf_ff"),
            (Regex::new(r"(?i)\bAflfair\b").unwrap(), "Affair", None, "lf_ff"),
            (Regex::new(r"(?i)\bAflfairs\b").unwrap(), "Affairs", None, "lf_ff"),
            (Regex::new(r"(?i)\bEflfect\b").unwrap(), "Effect", None, "lf_ff"),
            (Regex::new(r"(?i)\bEflfects\b").unwrap(), "Effects", None, "lf_ff"),
            (Regex::new(r"(?i)\bEflfective\b").unwrap(), "Effective", None, "lf_ff"),
            (Regex::new(r"(?i)\bEflfectively\b").unwrap(), "Effectively", None, "lf_ff"),
            (Regex::new(r"(?i)\bEflfort\b").unwrap(), "Effort", None, "lf_ff"),
            (Regex::new(r"(?i)\bEflforts\b").unwrap(), "Efforts", None, "lf_ff"),
            (Regex::new(r"(?i)\bOflf\b").unwrap(), "Off", None, "lf_ff"),
            (Regex::new(r"(?i)\bOflFice\b").unwrap(), "Office", None, "lf_ff"),
            (Regex::new(r"(?i)\bOflFicer\b").unwrap(), "Officer", None, "lf_ff"),
            (Regex::new(r"(?i)\bOflFicers\b").unwrap(), "Officers", None, "lf_ff"),
            (Regex::new(r"(?i)\bOflFicial\b").unwrap(), "Official", None, "lf_ff"),
            (Regex::new(r"(?i)\bFlfty\b").unwrap(), "Fifty", None, "lf_ff"),
            (Regex::new(r"(?i)\bFlft\b").unwrap(), "Fift", None, "lf_ff"),
            (Regex::new(r"(?i)\bFlfteen\b").unwrap(), "Fifteen", None, "lf_ff"),
            (Regex::new(r"(?i)\bFlfteenth\b").unwrap(), "Fifteenth", None, "lf_ff"),
            (Regex::new(r"(?i)\bFlfth\b").unwrap(), "Fifth", None, "lf_ff"),
            
            // Missing 'li' / gh patterns (English, slightly, etc)
            (Regex::new(r"(?i)\bEnghsh\b").unwrap(), "English", None, "missing_li"),
            (Regex::new(r"(?i)\bflightly\b").unwrap(), "slightly", None, "missing_li"),
            (Regex::new(r"(?i)\bShght\b").unwrap(), "Slight", None, "missing_li"),
            (Regex::new(r"(?i)\bShghtly\b").unwrap(), "Slightly", None, "missing_li"),
            (Regex::new(r"(?i)\bshghtest\b").unwrap(), "slightest", None, "missing_li"),
            (Regex::new(r"(?i)\bSUghtly\b").unwrap(), "Slightly", None, "missing_li"),
            (Regex::new(r"(?i)\bfprightly\b").unwrap(), "sprightly", None, "missing_li"),
            (Regex::new(r"(?i)\bHght\b").unwrap(), "Light", None, "missing_li"),
            (Regex::new(r"(?i)\bNght\b").unwrap(), "Night", None, "missing_li"),
            (Regex::new(r"(?i)\bMght\b").unwrap(), "Might", None, "missing_li"),
            (Regex::new(r"(?i)\bRght\b").unwrap(), "Right", None, "missing_li"),
            (Regex::new(r"(?i)\bDght\b").unwrap(), "Dight", None, "missing_li"),
            (Regex::new(r"(?i)\bfight\b").unwrap(), "sight", None, "missing_li"),
            
            // Combined Ull + mg patterns (double ligature break)
            (Regex::new(r"(?i)\bFoUowmg\b").unwrap(), "Following", None, "ull_mg_combined"),
            (Regex::new(r"(?i)\bFoUowiug\b").unwrap(), "Following", None, "ull_iug_combined"),
            (Regex::new(r"(?i)\bWiUmg\b").unwrap(), "Willing", None, "ull_mg_combined"),
            (Regex::new(r"(?i)\bKiUmg\b").unwrap(), "Killing", None, "ull_mg_combined"),
            (Regex::new(r"(?i)\bFiUmg\b").unwrap(), "Filling", None, "ull_mg_combined"),
            (Regex::new(r"(?i)\bPuUmg\b").unwrap(), "Pulling", None, "ull_mg_combined"),
            (Regex::new(r"(?i)\bCaUmg\b").unwrap(), "Calling", None, "ull_mg_combined"),
            (Regex::new(r"(?i)\bFaUmg\b").unwrap(), "Falling", None, "ull_mg_combined"),
            (Regex::new(r"(?i)\bTeUmg\b").unwrap(), "Telling", None, "ull_mg_combined"),
            (Regex::new(r"(?i)\bSeUmg\b").unwrap(), "Selling", None, "ull_mg_combined"),
            (Regex::new(r"(?i)\bDweUmg\b").unwrap(), "Dwelling", None, "ull_mg_combined"),
            (Regex::new(r"(?i)\bSweUmg\b").unwrap(), "Swelling", None, "ull_mg_combined"),
            
            // 'ing' showing as 'mg' (broken ligature)
            (Regex::new(r"(?i)\bThmg\b").unwrap(), "Thing", None, "ing_mg"),
            (Regex::new(r"(?i)\bThmgs\b").unwrap(), "Things", None, "ing_mg"),
            (Regex::new(r"(?i)\bBrmg\b").unwrap(), "Bring", None, "ing_mg"),
            (Regex::new(r"(?i)\bBrmgs\b").unwrap(), "Brings", None, "ing_mg"),
            (Regex::new(r"(?i)\bSprmg\b").unwrap(), "Spring", None, "ing_mg"),
            (Regex::new(r"(?i)\bAnythmg\b").unwrap(), "Anything", None, "ing_mg"),
            (Regex::new(r"(?i)\bEverythmg\b").unwrap(), "Everything", None, "ing_mg"),
            (Regex::new(r"(?i)\bSomethmg\b").unwrap(), "Something", None, "ing_mg"),
            (Regex::new(r"(?i)\bNothmg\b").unwrap(), "Nothing", None, "ing_mg"),
            (Regex::new(r"(?i)\bStnmg\b").unwrap(), "String", None, "ing_mg"),
            (Regex::new(r"(?i)\bStrmg\b").unwrap(), "String", None, "ing_mg"),
            (Regex::new(r"(?i)\bCarrymg\b").unwrap(), "Carrying", None, "ing_mg"),
            (Regex::new(r"(?i)\bFlymg\b").unwrap(), "Flying", None, "ing_mg"),
            (Regex::new(r"(?i)\bDymg\b").unwrap(), "Dying", None, "ing_mg"),
            (Regex::new(r"(?i)\bLymg\b").unwrap(), "Lying", None, "ing_mg"),
            (Regex::new(r"(?i)\bTrymg\b").unwrap(), "Trying", None, "ing_mg"),
            (Regex::new(r"(?i)\bCrymg\b").unwrap(), "Crying", None, "ing_mg"),
            (Regex::new(r"(?i)\bBegmnmg\b").unwrap(), "Beginning", None, "ing_mg"),
            (Regex::new(r"(?i)\bMornmg\b").unwrap(), "Morning", None, "ing_mg"),
            (Regex::new(r"(?i)\bEvenmg\b").unwrap(), "Evening", None, "ing_mg"),
            (Regex::new(r"(?i)\bMeanmg\b").unwrap(), "Meaning", None, "ing_mg"),
            (Regex::new(r"(?i)\bFeelmg\b").unwrap(), "Feeling", None, "ing_mg"),
            (Regex::new(r"(?i)\bMeetmg\b").unwrap(), "Meeting", None, "ing_mg"),
            (Regex::new(r"(?i)\bSaymg\b").unwrap(), "Saying", None, "ing_mg"),
            (Regex::new(r"(?i)\bPaymg\b").unwrap(), "Paying", None, "ing_mg"),
            (Regex::new(r"(?i)\bPlaymg\b").unwrap(), "Playing", None, "ing_mg"),
            (Regex::new(r"(?i)\bStaymg\b").unwrap(), "Staying", None, "ing_mg"),
            (Regex::new(r"(?i)\bLaymg\b").unwrap(), "Laying", None, "ing_mg"),
            
            // 'ing' showing as 'iiig' (broken ligature)
            (Regex::new(r"(?i)\bBeiiig\b").unwrap(), "Being", None, "ing_iiig"),
            (Regex::new(r"(?i)\bKiiig\b").unwrap(), "King", None, "ing_iiig"),
            (Regex::new(r"(?i)\bKiiigs\b").unwrap(), "Kings", None, "ing_iiig"),
            (Regex::new(r"(?i)\bThiiig\b").unwrap(), "Thing", None, "ing_iiig"),
            (Regex::new(r"(?i)\bThiiigs\b").unwrap(), "Things", None, "ing_iiig"),
            (Regex::new(r"(?i)\bBriiig\b").unwrap(), "Bring", None, "ing_iiig"),
            (Regex::new(r"(?i)\bMakiiig\b").unwrap(), "Making", None, "ing_iiig"),
            (Regex::new(r"(?i)\bTakiiig\b").unwrap(), "Taking", None, "ing_iiig"),
            (Regex::new(r"(?i)\bHaviiig\b").unwrap(), "Having", None, "ing_iiig"),
            (Regex::new(r"(?i)\bGiviiig\b").unwrap(), "Giving", None, "ing_iiig"),
            (Regex::new(r"(?i)\bLiviiig\b").unwrap(), "Living", None, "ing_iiig"),
            (Regex::new(r"(?i)\bDuriiig\b").unwrap(), "During", None, "ing_iiig"),
            (Regex::new(r"(?i)\bWritiiig\b").unwrap(), "Writing", None, "ing_iiig"),
            (Regex::new(r"(?i)\bFollowiiig\b").unwrap(), "Following", None, "ing_iiig"),
            (Regex::new(r"(?i)\bAccordiiig\b").unwrap(), "According", None, "ing_iiig"),
            (Regex::new(r"(?i)\bNothiiig\b").unwrap(), "Nothing", None, "ing_iiig"),
            (Regex::new(r"(?i)\bAnythiiig\b").unwrap(), "Anything", None, "ing_iiig"),
            (Regex::new(r"(?i)\bEverythiiig\b").unwrap(), "Everything", None, "ing_iiig"),
            (Regex::new(r"(?i)\bSomethiiig\b").unwrap(), "Something", None, "ing_iiig"),
            (Regex::new(r"(?i)\bMorniiig\b").unwrap(), "Morning", None, "ing_iiig"),
            (Regex::new(r"(?i)\bEveniiig\b").unwrap(), "Evening", None, "ing_iiig"),
            (Regex::new(r"(?i)\bMeaniiig\b").unwrap(), "Meaning", None, "ing_iiig"),
            (Regex::new(r"(?i)\bFeeliiig\b").unwrap(), "Feeling", None, "ing_iiig"),
            (Regex::new(r"(?i)\bMeetiiig\b").unwrap(), "Meeting", None, "ing_iiig"),
            (Regex::new(r"(?i)\bSeeiiig\b").unwrap(), "Seeing", None, "ing_iiig"),
            (Regex::new(r"(?i)\bGoiiig\b").unwrap(), "Going", None, "ing_iiig"),
            (Regex::new(r"(?i)\bDoiiig\b").unwrap(), "Doing", None, "ing_iiig"),
            (Regex::new(r"(?i)\bSayiiig\b").unwrap(), "Saying", None, "ing_iiig"),
            
            // cl/d confusion
            (
                Regex::new(r"(?i)\bclo\b").unwrap(),
                "do",
                Some(Regex::new(r"(?i)\b(to|not|can|will|shall|would|could)\s+clo\b").unwrap()),
                "cl_d_confusion"
            ),
            
            // Ligatures
            (Regex::new(r"ﬁ").unwrap(), "fi", None, "ligature"),
            (Regex::new(r"ﬂ").unwrap(), "fl", None, "ligature"),
            (Regex::new(r"ﬀ").unwrap(), "ff", None, "ligature"),
            (Regex::new(r"ﬃ").unwrap(), "ffi", None, "ligature"),
            (Regex::new(r"ﬄ").unwrap(), "ffl", None, "ligature"),
            
            // Google watermarks and digitization artifacts
            (Regex::new(r"(?i)\bVjOOQIC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bVjOOQLC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bVjOOQ\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bLjOOQIC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bLjOOQ\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bLiOOQLC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bCjOOQIC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bCjOOQlC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bCjOOQ\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bbyVjOOQlC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bbyVrrOOQlC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bbyCjOOQlC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bhyGoogIc\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bbyGoogk\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bbyGoogle\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bGoOglc\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bGoogXt\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bOOglC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bDigiLizedbyGoOglc\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)Digitized\s+by\s+[VLC]j?OOQ(?:IC|LC|lC)").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)Digitized\s+by\s+Google").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bdbyGoogle\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bbyGoogle\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bOOglC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bLiOOQ\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bVjOCK\b").unwrap(), "", None, "watermark"),
            // Anachronisms (modern terms that shouldn't appear in pre-WWI text)
            (Regex::new(r"(?i)\bgoogle\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\binternet\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"(?i)\bwebsite\b").unwrap(), "", None, "watermark"),
            
            // ff ligature errors (oflSce pattern)
            (Regex::new(r"(?i)\boflSce\b").unwrap(), "office", None, "ff_ligature"),
            (Regex::new(r"(?i)\boflScer\b").unwrap(), "officer", None, "ff_ligature"),
            (Regex::new(r"(?i)\boflScers\b").unwrap(), "officers", None, "ff_ligature"),
            (Regex::new(r"(?i)\boflScial\b").unwrap(), "official", None, "ff_ligature"),
            (Regex::new(r"(?i)\bdifTerent\b").unwrap(), "different", None, "ff_ligature"),
            (Regex::new(r"(?i)\bafTair\b").unwrap(), "affair", None, "ff_ligature"),
            (Regex::new(r"(?i)\bafTairs\b").unwrap(), "affairs", None, "ff_ligature"),
            (Regex::new(r"(?i)\bafTect\b").unwrap(), "affect", None, "ff_ligature"),
            (Regex::new(r"(?i)\befTect\b").unwrap(), "effect", None, "ff_ligature"),
            (Regex::new(r"(?i)\befTects\b").unwrap(), "effects", None, "ff_ligature"),
            
            // Repeated letters (AAA, BBB, etc) - expanded since Rust regex doesn't support backreferences
            (Regex::new(r"(?i)\bAAA+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bBBB+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bDDD+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bEEE+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bFFF+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bGGG+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bHHH+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bJJJ+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bKKK+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bNNN+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bOOO+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bPPP+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bQQQ+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bRRR+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bSSS+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bTTT+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bUUU+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bWWW+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bYYY+\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\bZZZ+\b").unwrap(), "", None, "ff_ligature"),
            
            // 2-letter noise
            (Regex::new(r"(?i)\b[I1]A\b").unwrap(), "", None, "ff_ligature"),
            (Regex::new(r"(?i)\b[I1]H\b").unwrap(), "", None, "ff_ligature"),
            
            // Word run-togethers (missing spaces between common words)
            // Case-insensitive to catch OCR variants like "oFthe"
            (Regex::new(r"(?i)\bofthe\b").unwrap(), "of the", None, "word_runtogether"),
            (Regex::new(r"(?i)\btothe\b").unwrap(), "to the", None, "word_runtogether"),
            (Regex::new(r"(?i)\binthe\b").unwrap(), "in the", None, "word_runtogether"),
            (Regex::new(r"(?i)\bforthe\b").unwrap(), "for the", None, "word_runtogether"),
            (Regex::new(r"(?i)\bonthe\b").unwrap(), "on the", None, "word_runtogether"),
            (Regex::new(r"(?i)\batthe\b").unwrap(), "at the", None, "word_runtogether"),
            (Regex::new(r"(?i)\bbythe\b").unwrap(), "by the", None, "word_runtogether"),
            (Regex::new(r"(?i)\bisthe\b").unwrap(), "is the", None, "word_runtogether"),
            (Regex::new(r"(?i)\bandthe\b").unwrap(), "and the", None, "word_runtogether"),
            (Regex::new(r"(?i)\bwiththe\b").unwrap(), "with the", None, "word_runtogether"),
            (Regex::new(r"(?i)\bfromthe\b").unwrap(), "from the", None, "word_runtogether"),
            
            // =================================================================
            // Long-s (ſ) OCR errors - f misread as long-s or vice versa
            // These are safe substitutions that are clearly OCR errors
            // Case-insensitive to catch OCR variants
            // =================================================================
            
            // Symptom family (very common in medical texts)
            (Regex::new(r"(?i)\bfymptom\b").unwrap(), "symptom", None, "long_s"),
            (Regex::new(r"(?i)\bfymptoms\b").unwrap(), "symptoms", None, "long_s"),
            (Regex::new(r"(?i)\bfymptomatic\b").unwrap(), "symptomatic", None, "long_s"),
            (Regex::new(r"(?i)\bfymptomatick\b").unwrap(), "symptomatic", None, "long_s"),
            (Regex::new(r"(?i)\bfymptomatical\b").unwrap(), "symptomatical", None, "long_s"),
            (Regex::new(r"(?i)\bfymptome\b").unwrap(), "symptome", None, "long_s"),
            (Regex::new(r"(?i)\bfymptomes\b").unwrap(), "symptomes", None, "long_s"),
            (Regex::new(r"(?i)\bfymptoma\b").unwrap(), "symptoma", None, "long_s"),
            
            // System family
            (Regex::new(r"(?i)\bfyftem\b").unwrap(), "system", None, "long_s"),
            (Regex::new(r"(?i)\bfyftems\b").unwrap(), "systems", None, "long_s"),
            (Regex::new(r"(?i)\bfyftematic\b").unwrap(), "systematic", None, "long_s"),
            (Regex::new(r"(?i)\bfyftcm\b").unwrap(), "system", None, "long_s"),
            
            // Majesty (common in historical/political texts)
            (Regex::new(r"(?i)\bmajefty\b").unwrap(), "majesty", None, "long_s"),
            (Regex::new(r"(?i)\bmajefty's\b").unwrap(), "majesty's", None, "long_s"),
            (Regex::new(r"(?i)\bmajefties\b").unwrap(), "majesties", None, "long_s"),
            
            // Suffer family
            (Regex::new(r"(?i)\bfuffer\b").unwrap(), "suffer", None, "long_s"),
            (Regex::new(r"(?i)\bfuffers\b").unwrap(), "suffers", None, "long_s"),
            (Regex::new(r"(?i)\bfuffered\b").unwrap(), "suffered", None, "long_s"),
            (Regex::new(r"(?i)\bfuffering\b").unwrap(), "suffering", None, "long_s"),
            (Regex::new(r"(?i)\bfufFers\b").unwrap(), "suffers", None, "long_s"),
            (Regex::new(r"(?i)\bfufTer\b").unwrap(), "suffer", None, "long_s"),
            
            // Sufficient family
            (Regex::new(r"(?i)\bfufficient\b").unwrap(), "sufficient", None, "long_s"),
            (Regex::new(r"(?i)\bfufficiently\b").unwrap(), "sufficiently", None, "long_s"),
            (Regex::new(r"(?i)\bfufEcient\b").unwrap(), "sufficient", None, "long_s"),
            (Regex::new(r"(?i)\bfufEciently\b").unwrap(), "sufficiently", None, "long_s"),
            (Regex::new(r"(?i)\binfufficient\b").unwrap(), "insufficient", None, "long_s"),
            
            // Suspect family
            (Regex::new(r"(?i)\bfufpect\b").unwrap(), "suspect", None, "long_s"),
            (Regex::new(r"(?i)\bfufpected\b").unwrap(), "suspected", None, "long_s"),
            (Regex::new(r"(?i)\bfufpecT\b").unwrap(), "suspect", None, "long_s"),
            (Regex::new(r"(?i)\bfufpicion\b").unwrap(), "suspicion", None, "long_s"),
            
            // Satisfy family
            (Regex::new(r"(?i)\bfatisfy\b").unwrap(), "satisfy", None, "long_s"),
            (Regex::new(r"(?i)\bfatisfied\b").unwrap(), "satisfied", None, "long_s"),
            (Regex::new(r"(?i)\bfatisfaction\b").unwrap(), "satisfaction", None, "long_s"),
            (Regex::new(r"(?i)\bfatisfactory\b").unwrap(), "satisfactory", None, "long_s"),
            
            // Substance family
            (Regex::new(r"(?i)\bfubftance\b").unwrap(), "substance", None, "long_s"),
            (Regex::new(r"(?i)\bfubftances\b").unwrap(), "substances", None, "long_s"),
            (Regex::new(r"(?i)\bfubftantial\b").unwrap(), "substantial", None, "long_s"),
            
            // Subject family
            (Regex::new(r"(?i)\bfubject\b").unwrap(), "subject", None, "long_s"),
            (Regex::new(r"(?i)\bfubjects\b").unwrap(), "subjects", None, "long_s"),
            (Regex::new(r"(?i)\bfubjcft\b").unwrap(), "subject", None, "long_s"),
            
            // Success family
            (Regex::new(r"(?i)\bfuccefs\b").unwrap(), "success", None, "long_s"),
            (Regex::new(r"(?i)\bfuccefful\b").unwrap(), "successful", None, "long_s"),
            (Regex::new(r"(?i)\bfucceffion\b").unwrap(), "succession", None, "long_s"),
            (Regex::new(r"(?i)\bfucceffive\b").unwrap(), "successive", None, "long_s"),
            
            // Such
            (Regex::new(r"(?i)\bfuch\b").unwrap(), "such", None, "long_s"),
            
            // Surface
            (Regex::new(r"(?i)\bfurface\b").unwrap(), "surface", None, "long_s"),
            (Regex::new(r"(?i)\bfurfaces\b").unwrap(), "surfaces", None, "long_s"),
            
            // Strength (with additional OCR corruption)
            (Regex::new(r"(?i)\bftrcngth\b").unwrap(), "strength", None, "long_s"),
            (Regex::new(r"(?i)\bftrength\b").unwrap(), "strength", None, "long_s"),
            
            // First (common OCR error)
            (Regex::new(r"(?i)\bfyrft\b").unwrap(), "first", None, "long_s"),
            (Regex::new(r"(?i)\bfirft\b").unwrap(), "first", None, "long_s"),
            
            // Themselves, himself, herself, etc. with long-s corruption
            (Regex::new(r"(?i)\bthemfelves\b").unwrap(), "themselves", None, "long_s"),
            (Regex::new(r"(?i)\bthcmfelves\b").unwrap(), "themselves", None, "long_s"),
            (Regex::new(r"(?i)\bhimfelf\b").unwrap(), "himself", None, "long_s"),
            (Regex::new(r"(?i)\bherfelf\b").unwrap(), "herself", None, "long_s"),
            (Regex::new(r"(?i)\bitfelf\b").unwrap(), "itself", None, "long_s"),
            (Regex::new(r"(?i)\bmyfelf\b").unwrap(), "myself", None, "long_s"),
            (Regex::new(r"(?i)\byourfelf\b").unwrap(), "yourself", None, "long_s"),
            (Regex::new(r"(?i)\bourfelf\b").unwrap(), "ourself", None, "long_s"),
            (Regex::new(r"(?i)\bourfelves\b").unwrap(), "ourselves", None, "long_s"),
            
            // =================================================================
            // Ligature corruption: fiil -> ful (fi ligature + u mangled)
            // Common English -ful suffix words where OCR rendered 'ful' as 'fiil'
            // =================================================================
            (Regex::new(r"(?i)\bbeautifiil\b").unwrap(), "beautiful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bbeautifiilly\b").unwrap(), "beautifully", None, "fiil_ful"),
            (Regex::new(r"(?i)\busefiil\b").unwrap(), "useful", None, "fiil_ful"),
            (Regex::new(r"(?i)\busefiilness\b").unwrap(), "usefulness", None, "fiil_ful"),
            (Regex::new(r"(?i)\bpowerfiil\b").unwrap(), "powerful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bpowerfiilly\b").unwrap(), "powerfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bawfiil\b").unwrap(), "awful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bawfiilly\b").unwrap(), "awfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bcarefiil\b").unwrap(), "careful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bcarefiilly\b").unwrap(), "carefully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bcareftil\b").unwrap(), "careful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bcareftilly\b").unwrap(), "carefully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bsuccessfiil\b").unwrap(), "successful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bsuccessfiilly\b").unwrap(), "successfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bfaithfiil\b").unwrap(), "faithful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bfaithfiilly\b").unwrap(), "faithfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\blawfiil\b").unwrap(), "lawful", None, "fiil_ful"),
            (Regex::new(r"(?i)\blawfiilly\b").unwrap(), "lawfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bpainfiil\b").unwrap(), "painful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bpainfiilly\b").unwrap(), "painfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bdoubtfiil\b").unwrap(), "doubtful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bdreadfiil\b").unwrap(), "dreadful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bdreadfiilly\b").unwrap(), "dreadfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bfearfiil\b").unwrap(), "fearful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bfearfiilly\b").unwrap(), "fearfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bgratefiil\b").unwrap(), "grateful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bgratefiilly\b").unwrap(), "gratefully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bgracefiil\b").unwrap(), "graceful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bgracefiilly\b").unwrap(), "gracefully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bpeacefiil\b").unwrap(), "peaceful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bpeacefiilly\b").unwrap(), "peacefully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bneedfiil\b").unwrap(), "needful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bskilfiil\b").unwrap(), "skilful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bskilfiilly\b").unwrap(), "skilfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\byouthfiil\b").unwrap(), "youthful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bsinfiil\b").unwrap(), "sinful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bmercifiil\b").unwrap(), "merciful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bmercifiilly\b").unwrap(), "mercifully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bjoyfiil\b").unwrap(), "joyful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bjoyfiilly\b").unwrap(), "joyfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bthankfiil\b").unwrap(), "thankful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bthankfiilly\b").unwrap(), "thankfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bunlawfiil\b").unwrap(), "unlawful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bunlawfiilly\b").unwrap(), "unlawfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bwilfiil\b").unwrap(), "wilful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bwilfiilly\b").unwrap(), "wilfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bfruitfiil\b").unwrap(), "fruitful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bplentifiil\b").unwrap(), "plentiful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bplentifiilly\b").unwrap(), "plentifully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bfrightfiil\b").unwrap(), "frightful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bhandfiil\b").unwrap(), "handful", None, "fiil_ful"),
            (Regex::new(r"(?i)\brespectfiil\b").unwrap(), "respectful", None, "fiil_ful"),
            (Regex::new(r"(?i)\brespectfiilly\b").unwrap(), "respectfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bunsuccessfiil\b").unwrap(), "unsuccessful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bwonderfiil\b").unwrap(), "wonderful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bwonderfiilly\b").unwrap(), "wonderfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bhopefiil\b").unwrap(), "hopeful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bhelpfiil\b").unwrap(), "helpful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bharmfiil\b").unwrap(), "harmful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bhatefiil\b").unwrap(), "hateful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bshamefiil\b").unwrap(), "shameful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bdisgracefiil\b").unwrap(), "disgraceful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bcheerfiil\b").unwrap(), "cheerful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bcheerfiilly\b").unwrap(), "cheerfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bwatchfiil\b").unwrap(), "watchful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bdeceitfiil\b").unwrap(), "deceitful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bboastfiil\b").unwrap(), "boastful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bthoughtfiil\b").unwrap(), "thoughtful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bdistressfiil\b").unwrap(), "distressful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bdistrustfiil\b").unwrap(), "distrustful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bremorsefiil\b").unwrap(), "remorseful", None, "fiil_ful"),
            (Regex::new(r"(?i)\breproachfiil\b").unwrap(), "reproachful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bresentfiil\b").unwrap(), "resentful", None, "fiil_ful"),
            (Regex::new(r"(?i)\brestfiil\b").unwrap(), "restful", None, "fiil_ful"),
            (Regex::new(r"(?i)\brevengefiil\b").unwrap(), "revengeful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bscornfiil\b").unwrap(), "scornful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bscornfiilly\b").unwrap(), "scornfully", None, "fiil_ful"),
            (Regex::new(r"(?i)\bspitefiil\b").unwrap(), "spiteful", None, "fiil_ful"),
            (Regex::new(r"(?i)\btastefiil\b").unwrap(), "tasteful", None, "fiil_ful"),
            (Regex::new(r"(?i)\btruthfiil\b").unwrap(), "truthful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bwistfiil\b").unwrap(), "wistful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bwrathfiil\b").unwrap(), "wrathful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bfancifiil\b").unwrap(), "fanciful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bbountifiil\b").unwrap(), "bountiful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bdutifiil\b").unwrap(), "dutiful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bpitifiil\b").unwrap(), "pitiful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bplayfiil\b").unwrap(), "playful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bprayerfiil\b").unwrap(), "prayerful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bmournfiil\b").unwrap(), "mournful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bdolefiil\b").unwrap(), "doleful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bwoefiil\b").unwrap(), "woeful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bforgetfiil\b").unwrap(), "forgetful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bneglectfiil\b").unwrap(), "neglectful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bungratefiil\b").unwrap(), "ungrateful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bunmindfiil\b").unwrap(), "unmindful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bunfaithfiil\b").unwrap(), "unfaithful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bhealthfiil\b").unwrap(), "healthful", None, "fiil_ful"),
            // Measurement words with fiil
            (Regex::new(r"(?i)\bteaspoonfiil\b").unwrap(), "teaspoonful", None, "fiil_ful"),
            (Regex::new(r"(?i)\btablespoonfiil\b").unwrap(), "tablespoonful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bspoonfiil\b").unwrap(), "spoonful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bcupfiil\b").unwrap(), "cupful", None, "fiil_ful"),
            (Regex::new(r"(?i)\bmouthfiil\b").unwrap(), "mouthful", None, "fiil_ful"),
            
            // =================================================================
            // Additional word-joining patterns (The+word, common run-togethers)
            // =================================================================
            (Regex::new(r"(?i)\btheCity\b").unwrap(), "the City", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheKing\b").unwrap(), "the King", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheQueen\b").unwrap(), "the Queen", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheLord\b").unwrap(), "the Lord", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheWorld\b").unwrap(), "the World", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheState\b").unwrap(), "the State", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheChurch\b").unwrap(), "the Church", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheHouse\b").unwrap(), "the House", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheGovernment\b").unwrap(), "the Government", None, "word_runtogether"),
            (Regex::new(r"(?i)\bthePeople\b").unwrap(), "the People", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheUnited\b").unwrap(), "the United", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheSame\b").unwrap(), "the same", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheOther\b").unwrap(), "the other", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheFirst\b").unwrap(), "the first", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheLast\b").unwrap(), "the last", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheGreat\b").unwrap(), "the Great", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheNew\b").unwrap(), "the New", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheOld\b").unwrap(), "the Old", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheWhole\b").unwrap(), "the whole", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheMost\b").unwrap(), "the most", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheBest\b").unwrap(), "the best", None, "word_runtogether"),
            (Regex::new(r"(?i)\btheOnly\b").unwrap(), "the only", None, "word_runtogether"),
            (Regex::new(r"(?i)\bitis\b").unwrap(), "it is", None, "word_runtogether"),
            (Regex::new(r"(?i)\bitwas\b").unwrap(), "it was", None, "word_runtogether"),
            (Regex::new(r"(?i)\btobe\b").unwrap(), "to be", None, "word_runtogether"),
            (Regex::new(r"(?i)\bofit\b").unwrap(), "of it", None, "word_runtogether"),
            (Regex::new(r"(?i)\bifthe\b").unwrap(), "if the", None, "word_runtogether"),
            (Regex::new(r"(?i)\basthe\b").unwrap(), "as the", None, "word_runtogether"),
            (Regex::new(r"(?i)\borthe\b").unwrap(), "or the", None, "word_runtogether"),
            (Regex::new(r"(?i)\bofhis\b").unwrap(), "of his", None, "word_runtogether"),
            (Regex::new(r"(?i)\bofher\b").unwrap(), "of her", None, "word_runtogether"),
            (Regex::new(r"(?i)\btothis\b").unwrap(), "to this", None, "word_runtogether"),
            (Regex::new(r"(?i)\binthis\b").unwrap(), "in this", None, "word_runtogether"),
            (Regex::new(r"(?i)\bofthis\b").unwrap(), "of this", None, "word_runtogether"),
            
            // =================================================================
            // Additional ii/u confusion patterns
            // =================================================================
            (Regex::new(r"(?i)\bfiill\b").unwrap(), "full", None, "ii_u_confusion"),
            (Regex::new(r"(?i)\bfiilly\b").unwrap(), "fully", None, "ii_u_confusion"),
            (Regex::new(r"(?i)\bwiil\b").unwrap(), "will", None, "ii_u_confusion"),
            (Regex::new(r"(?i)\bwiill\b").unwrap(), "will", None, "ii_u_confusion"),
            (Regex::new(r"\biiis\b").unwrap(), "his", None, "ii_u_confusion"),
            (Regex::new(r"\bIiis\b").unwrap(), "His", None, "ii_u_confusion"),
            (Regex::new(r"\biiim\b").unwrap(), "him", None, "ii_u_confusion"),
            (Regex::new(r"\bIiim\b").unwrap(), "Him", None, "ii_u_confusion"),
            (Regex::new(r"(?i)\bhiis\b").unwrap(), "his", None, "ii_u_confusion"),
            (Regex::new(r"(?i)\bliimself\b").unwrap(), "himself", None, "ii_u_confusion"),
            (Regex::new(r"(?i)\bliini\b").unwrap(), "him", None, "ii_u_confusion"),
            (Regex::new(r"(?i)\bliia\b").unwrap(), "his", None, "ii_u_confusion"),
            (Regex::new(r"(?i)\bdiiferent\b").unwrap(), "different", None, "ii_u_confusion"),
            (Regex::new(r"(?i)\bdiifferent\b").unwrap(), "different", None, "ii_u_confusion"),
            
            // =================================================================
            // Additional Google watermark variants
            // =================================================================
            (Regex::new(r"\bVjOOQIC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"\bVjOOQLC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"\bLjOOQIC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"\bLiOOQLC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"\bCjOOQIC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"\bCjOOQlC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"\bbyVjOOQlC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"\bbyVrrOOQlC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"\bbyCjOOQlC\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"\bhyGoogIc\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"\bGoOglc\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"\bGoogXt\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"\bDigiLizedbyGoOglc\b").unwrap(), "", None, "watermark"),
            (Regex::new(r"Digitized\s+by\s+[VLC]j?OOQ(?:IC|LC|lC)").unwrap(), "", None, "watermark"),
        ]
    };
    
    // ==========================================================================
    // Context-dependent patterns - COUNT only, don't auto-correct
    // These need human review or surrounding context to determine correct fix
    // ==========================================================================
    static ref CONTEXT_PATTERNS: Vec<(&'static str, Regex, &'static str)> = {
        vec![
            // lie -> he (only correct after conjunctions/pronouns)
            // "and lie" -> "and he", but "to lie down" should stay
            ("lie_to_he", Regex::new(r"(?i)\blie\b").unwrap(), "he"),
            
            // Historical spellings that MIGHT be OCR errors
            // publick -> public (valid 17th-18th century)
            ("publick", Regex::new(r"(?i)\bpublick\b").unwrap(), "public"),
            // untill -> until (valid pre-1700)
            ("untill", Regex::new(r"(?i)\buntill\b").unwrap(), "until"),
            // chuse -> choose (valid 18th century)
            ("chuse", Regex::new(r"(?i)\bchuse\b").unwrap(), "choose"),
            // shew -> show (valid pre-1800)
            ("shew", Regex::new(r"(?i)\bshew\b").unwrap(), "show"),
            // connexion -> connection (British historical)
            ("connexion", Regex::new(r"(?i)\bconnexion\b").unwrap(), "connection"),
            
            // Words that could be names or OCR errors
            // horne -> home (but Horne is a surname)
            ("horne", Regex::new(r"(?i)\bhorne\b").unwrap(), "home"),
            
            // HaUe is highly ambiguous (12k+ occurrences):
            // - "Halle" (German city/hall, e.g., "Gambrinus-Halle") - keep
            // - "Have" (OCR error) - fix to "have"  
            // - "Hall" (OCR error) - fix to "hall"
            // Needs context to determine correct action
            ("HaUe_ambiguous", Regex::new(r"(?i)\bhaUe\b").unwrap(), "have/halle/hall"),
        ]
    };
}

/// Result of OCR cleanup with category breakdown
#[pyclass]
#[derive(Clone)]
pub struct CleanupResult {
    #[pyo3(get)]
    pub text: String,
    #[pyo3(get)]
    pub total_substitutions: u64,
    #[pyo3(get)]
    pub substitutions_by_category: std::collections::HashMap<String, u64>,
}

/// Get the number of loaded OCR patterns (for debugging)
#[pyfunction]
fn pattern_count() -> usize {
    OCR_PATTERNS.len()
}

/// Clean OCR errors in text using pre-compiled patterns
#[pyfunction]
fn clean_text(text: String) -> PyResult<(String, u64)> {
    let (result, subs, _categories) = clean_text_internal(&text);
    Ok((result, subs))
}

/// Clean OCR errors in text and return detailed category breakdown
#[pyfunction]
fn clean_text_with_categories(text: String) -> PyResult<CleanupResult> {
    let (result, subs, categories) = clean_text_internal(&text);
    Ok(CleanupResult {
        text: result,
        total_substitutions: subs,
        substitutions_by_category: categories,
    })
}

/// Count context-dependent patterns (patterns that need review, not auto-corrected)
/// Returns: HashMap<pattern_name, count>
#[pyfunction]
fn count_context_patterns(text: String) -> PyResult<std::collections::HashMap<String, u64>> {
    let mut counts: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    
    for (name, pattern, _potential_fix) in CONTEXT_PATTERNS.iter() {
        let count = pattern.find_iter(&text).count() as u64;
        if count > 0 {
            counts.insert(name.to_string(), count);
        }
    }
    
    Ok(counts)
}

/// Count context-dependent patterns in a file
/// Returns: HashMap<pattern_name, count>
#[pyfunction]
fn count_context_patterns_file(file_path: String) -> PyResult<std::collections::HashMap<String, u64>> {
    use std::fs;
    
    let content = fs::read_to_string(&file_path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Failed to read {}: {}", file_path, e)))?;
    
    let mut counts: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    
    for (name, pattern, _potential_fix) in CONTEXT_PATTERNS.iter() {
        let count = pattern.find_iter(&content).count() as u64;
        if count > 0 {
            counts.insert(name.to_string(), count);
        }
    }
    
    Ok(counts)
}

/// Batch count context-dependent patterns across multiple files
/// Returns: HashMap<pattern_name, total_count>
#[pyfunction]
fn count_context_patterns_batch(file_paths: Vec<String>) -> PyResult<std::collections::HashMap<String, u64>> {
    use std::fs;
    
    let mut totals: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    
    for file_path in file_paths {
        let content = match fs::read_to_string(&file_path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        
        for (name, pattern, _potential_fix) in CONTEXT_PATTERNS.iter() {
            let count = pattern.find_iter(&content).count() as u64;
            if count > 0 {
                *totals.entry(name.to_string()).or_insert(0) += count;
            }
        }
    }
    
    Ok(totals)
}

/// Clean a single file, reading and writing entirely in Rust
/// Pipeline: strip boilerplate -> OCR cleanup -> write output
/// Returns: (was_modified, substitution_count, bytes_read, categories, boilerplate_regions)
/// where categories is a HashMap of category_name -> count
/// and boilerplate_regions is a list of (category, pattern_name, start_line, end_line, char_count)
#[pyfunction]
fn clean_file_to_file(input_path: String, output_path: String) -> PyResult<(bool, u64, u64, std::collections::HashMap<String, u64>, Vec<(String, String, usize, usize, usize)>)> {
    use std::fs;
    use std::path::Path;

    // Read file
    let content = fs::read_to_string(&input_path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Failed to read {}: {}", input_path, e)))?;
    
    let bytes_read = content.len() as u64;
    
    // Step 1: Strip boilerplate (digitization notices, library stamps, etc.)
    let (stripped_content, boilerplate_regions) = strip_boilerplate_internal(&content);
    
    // Convert boilerplate regions to tuple format for Python
    let boilerplate_tuples: Vec<(String, String, usize, usize, usize)> = boilerplate_regions
        .iter()
        .map(|r| (r.category.clone(), r.pattern_name.clone(), r.start_line, r.end_line, r.char_count))
        .collect();
    
    // Step 2: OCR cleanup on stripped content
    let (cleaned, subs, categories) = clean_text_internal(&stripped_content);
    
    // Document was modified if we stripped boilerplate OR made OCR substitutions
    let was_modified = !boilerplate_regions.is_empty() || subs > 0;

    // Ensure parent directory exists
    let out_path = Path::new(&output_path);
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Failed to create directory: {}", e)))?;
    }

    // Write output
    fs::write(out_path, &cleaned)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Failed to write {}: {}", output_path, e)))?;

    Ok((was_modified, subs, bytes_read, categories, boilerplate_tuples))
}

/// Statistics for a batch of files processed in parallel
#[pyclass]
#[derive(Clone)]
struct BatchStats {
    #[pyo3(get)]
    files_processed: usize,
    #[pyo3(get)]
    files_modified: usize,
    #[pyo3(get)]
    files_failed: usize,
    #[pyo3(get)]
    total_substitutions: u64,
    #[pyo3(get)]
    total_bytes: u64,
    #[pyo3(get)]
    long_s_fixes: u64,
    #[pyo3(get)]
    boilerplate_files: usize,
    #[pyo3(get)]
    boilerplate_chars: u64,
}

#[pymethods]
impl BatchStats {
    fn __repr__(&self) -> String {
        format!(
            "BatchStats(processed={}, modified={}, failed={}, subs={}, bytes={})",
            self.files_processed, self.files_modified, self.files_failed,
            self.total_substitutions, self.total_bytes
        )
    }
}

/// Process multiple files in parallel using Rayon
/// 
/// Args:
///     file_pairs: List of (input_path, output_path) tuples
///     num_threads: Number of threads to use (default: 24)
/// 
/// Returns:
///     BatchStats with aggregated statistics
#[pyfunction]
#[pyo3(signature = (file_pairs, num_threads=None))]
fn clean_batch_parallel(
    file_pairs: Vec<(String, String)>,
    num_threads: Option<usize>,
) -> PyResult<BatchStats> {
    use std::fs;
    use std::path::Path;
    use std::collections::HashSet;
    
    let threads = num_threads.unwrap_or(24);
    
    // Configure thread pool (only if not already set)
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()
        .ok(); // Ignore error if already initialized
    
    // Pre-create all output directories (single-threaded to avoid races)
    let output_dirs: HashSet<_> = file_pairs
        .iter()
        .filter_map(|(_, output)| Path::new(output).parent().map(|p| p.to_path_buf()))
        .collect();
    
    for dir in &output_dirs {
        fs::create_dir_all(dir).ok();
    }
    
    // Atomic counters for thread-safe aggregation
    let files_processed = AtomicUsize::new(0);
    let files_modified = AtomicUsize::new(0);
    let files_failed = AtomicUsize::new(0);
    let total_substitutions = AtomicU64::new(0);
    let total_bytes = AtomicU64::new(0);
    let long_s_fixes = AtomicU64::new(0);
    let boilerplate_files = AtomicUsize::new(0);
    let boilerplate_chars = AtomicU64::new(0);
    
    // Process files in parallel
    file_pairs.par_iter().for_each(|(input_path, output_path)| {
        match clean_file_internal(input_path, output_path) {
            Ok((was_modified, subs, bytes, categories, bp_regions)) => {
                files_processed.fetch_add(1, Ordering::Relaxed);
                total_bytes.fetch_add(bytes, Ordering::Relaxed);
                total_substitutions.fetch_add(subs, Ordering::Relaxed);
                
                if was_modified {
                    files_modified.fetch_add(1, Ordering::Relaxed);
                }
                
                if let Some(ls) = categories.get("long_s") {
                    long_s_fixes.fetch_add(*ls, Ordering::Relaxed);
                }
                
                if !bp_regions.is_empty() {
                    boilerplate_files.fetch_add(1, Ordering::Relaxed);
                    let bp_chars: usize = bp_regions.iter().map(|r| r.char_count).sum();
                    boilerplate_chars.fetch_add(bp_chars as u64, Ordering::Relaxed);
                }
            }
            Err(e) => {
                files_failed.fetch_add(1, Ordering::Relaxed);
                eprintln!("Error processing {}: {}", input_path, e);
            }
        }
    });
    
    Ok(BatchStats {
        files_processed: files_processed.load(Ordering::Relaxed),
        files_modified: files_modified.load(Ordering::Relaxed),
        files_failed: files_failed.load(Ordering::Relaxed),
        total_substitutions: total_substitutions.load(Ordering::Relaxed),
        total_bytes: total_bytes.load(Ordering::Relaxed),
        long_s_fixes: long_s_fixes.load(Ordering::Relaxed),
        boilerplate_files: boilerplate_files.load(Ordering::Relaxed),
        boilerplate_chars: boilerplate_chars.load(Ordering::Relaxed),
    })
}

/// Internal file processing (returns Result for error handling in parallel context)
fn clean_file_internal(
    input_path: &str,
    output_path: &str,
) -> Result<(bool, u64, u64, std::collections::HashMap<String, u64>, Vec<StrippedRegion>), String> {
    use std::fs;
    use std::path::Path;
    
    // Read file
    let content = fs::read_to_string(input_path)
        .map_err(|e| format!("Failed to read {}: {}", input_path, e))?;
    
    let bytes_read = content.len() as u64;
    
    // Step 1: Strip boilerplate
    let (stripped_content, boilerplate_regions) = strip_boilerplate_internal(&content);
    
    // Step 2: OCR cleanup
    let (cleaned, subs, categories) = clean_text_internal(&stripped_content);
    
    let was_modified = !boilerplate_regions.is_empty() || subs > 0;
    
    // Ensure parent directory exists (should already exist from pre-creation, but be safe)
    let out_path = Path::new(output_path);
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create directory: {}", e))?;
    }
    
    // Write output
    fs::write(out_path, &cleaned)
        .map_err(|e| format!("Failed to write {}: {}", output_path, e))?;
    
    Ok((was_modified, subs, bytes_read, categories, boilerplate_regions))
}

/// Internal clean function (not exposed to Python, avoids string copies)
/// Returns: (cleaned_text, total_substitutions, substitutions_by_category)
fn clean_text_internal(text: &str) -> (String, u64, std::collections::HashMap<String, u64>) {
    use std::collections::HashMap;
    
    // Phase 0: Line unwrapping (dehyphenation + join cosmetic line breaks)
    // This must run BEFORE OCR pattern substitutions so patterns can match complete words
    let (unwrapped, _lines_joined, _words_dehyphenated, _spaces_normalized) = unwrap_lines_internal(text);
    
    let mut result = unwrapped;
    let mut total_subs: u64 = 0;
    let mut category_counts: HashMap<String, u64> = HashMap::new();

    for (pattern, replacement, context, category) in OCR_PATTERNS.iter() {
        if let Some(ctx_pattern) = context {
            // Contextual replacement - only replace if context matches
            let mut match_count: u64 = 0;
            result = ctx_pattern.replace_all(&result, |caps: &regex::Captures| {
                let matched = caps.get(0).unwrap().as_str();
                let replaced = pattern.replace_all(matched, *replacement);
                if replaced != matched {
                    match_count += 1;
                }
                replaced.into_owned()
            }).into_owned();
            if match_count > 0 {
                total_subs += match_count;
                *category_counts.entry(category.to_string()).or_insert(0) += match_count;
            }
        } else {
            // Direct replacement
            let before_count = pattern.find_iter(&result).count();
            if before_count > 0 {
                result = pattern.replace_all(&result, *replacement).into_owned();
                total_subs += before_count as u64;
                *category_counts.entry(category.to_string()).or_insert(0) += before_count as u64;
            }
        }
    }

    (result, total_subs, category_counts)
}

// =============================================================================
// Vocabulary Extraction
// =============================================================================

// User-provided whitelist (known good words to skip)
static WHITELIST: std::sync::LazyLock<std::sync::RwLock<std::collections::HashSet<String>>> = 
    std::sync::LazyLock::new(|| std::sync::RwLock::new(std::collections::HashSet::new()));

/// Initialize the whitelist with known good words (called from Python)
#[pyfunction]
fn init_whitelist(words: Vec<String>) -> PyResult<usize> {
    let mut whitelist = WHITELIST.write().map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to acquire whitelist lock: {}", e))
    })?;
    whitelist.clear();
    for word in &words {
        whitelist.insert(word.to_lowercase());
    }
    let count = whitelist.len();
    Ok(count)
}

/// Check if a word is in the whitelist (HashSet or pattern-based)
fn is_whitelisted(word: &str) -> bool {
    let word_lower = word.to_lowercase();
    
    // 1. Check explicit whitelist (loaded from file)
    if let Ok(whitelist) = WHITELIST.read() {
        if whitelist.contains(&word_lower) {
            return true;
        }
    }
    
    // 2. Check pattern-based whitelist rules
    
    // Scottish/Irish names: McDonald, MacArthur, McIntyre
    if WHITELIST_MC_NAMES.is_match(word) {
        return true;
    }
    
    // British -ise spelling variants
    if WHITELIST_BRITISH_ISE.is_match(word) {
        return true;
    }
    
    // Chemical formulas (case-sensitive check)
    if CHEMICAL_FORMULAS.contains(word) {
        return true;
    }
    
    false
}

lazy_static! {
    // Word extraction pattern
    static ref WORD_PATTERN: Regex = Regex::new(r"\b([a-zA-Z][a-zA-Z']*[a-zA-Z]|[a-zA-Z])\b").unwrap();
    
    // Suspicious patterns that suggest OCR errors
    // Category codes: M=mixed_case, R=repeated, X=modern, F=fragment, G=garbage, C=confusable
    // Note: Rust regex doesn't support backreferences, so we enumerate repeated chars
    static ref SUSPICIOUS_PATTERNS: Vec<(Regex, &'static str)> = vec![
        // M: Mixed case OCR garbage (camelCase in middle of word)
        (Regex::new(r"[a-z][A-Z]").unwrap(), "M:mixed_case"),
        
        // R: Repeated characters (triple+ repeats)
        (Regex::new(r"(?i)(aaa|bbb|ccc|ddd|eee|fff|ggg|hhh|iii|jjj|kkk|lll|mmm|nnn|ooo|ppp|qqq|rrr|sss|ttt|uuu|vvv|www|xxx|yyy|zzz)").unwrap(), "R:triple_repeat"),
        
        // G: Garbage patterns (consonant runs, all consonants)
        (Regex::new(r"[^aeiouAEIOU]{5,}").unwrap(), "G:consonant_run"),
        (Regex::new(r"(?i)^[bcdfghjklmnpqrstvwxz]{4,}$").unwrap(), "G:all_consonants"),
        
        // C: Confusable char sequences - require actual OCR confusion markers (digits or pipe)
        (Regex::new(r"[1|][il1|]+").unwrap(), "C:digit_confusion"),
        (Regex::new(r"[il1|]+[1|]").unwrap(), "C:digit_confusion"),
        (Regex::new(r"[rnm]{4,}").unwrap(), "C:rn_m_confusion"),
        
        // X: Modern contamination (URLs, tech terms that shouldn't be in pre-WWI texts)
        (Regex::new(r"(?i)^https?$").unwrap(), "X:url_protocol"),
        (Regex::new(r"(?i)^www$").unwrap(), "X:url_www"),
        (Regex::new(r"(?i)\.com$|\.org$|\.net$|\.edu$|\.gov$").unwrap(), "X:url_domain"),
        (Regex::new(r"(?i)@[a-z]+\.[a-z]+").unwrap(), "X:email"),
        (Regex::new(r"(?i)^(javascript|html|xml|pdf|jpg|png|gif|php|asp|css)$").unwrap(), "X:file_ext"),
        (Regex::new(r"(?i)^(google|facebook|twitter|youtube|wikipedia|amazon|ebay|paypal)$").unwrap(), "X:modern_brand"),
        (Regex::new(r"(?i)^(digitized|digitised|scanned|uploaded|downloaded|online|offline|internet|website|webpage|email|smartphone|computer)$").unwrap(), "X:modern_tech"),
        
        // F: Fragments (truncated words - common suffixes/prefixes appearing alone)
        (Regex::new(r"(?i)^(tion|tions|ment|ments|ness|ling|lings|ful|less|able|ible|ous|ious|ing|ings|ity|ities)$").unwrap(), "F:suffix_fragment"),
        (Regex::new(r"(?i)^(pre|pro|anti|non|sub|super|trans|inter|intra|extra|ultra|semi|multi)$").unwrap(), "F:prefix_fragment"),
    ];
    
    // Common words to skip (too common to be interesting)
    static ref SKIP_WORDS: std::collections::HashSet<&'static str> = {
        let words = [
            "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with",
            "by", "from", "as", "is", "was", "are", "were", "been", "be", "have", "has", "had",
            "do", "does", "did", "will", "would", "could", "should", "may", "might", "must",
            "shall", "can", "need", "this", "that", "these", "those", "it", "its", "he", "she",
            "they", "him", "her", "them", "his", "their", "my", "your", "our", "who", "which",
            "what", "where", "when", "why", "how", "all", "each", "every", "both", "few", "more",
            "most", "other", "some", "such", "no", "not", "only", "same", "so", "than", "too",
            "very", "just", "also", "now", "i", "you", "we", "me", "us",
        ];
        words.iter().cloned().collect()
    };
    
    // ==========================================================================
    // WHITELIST PATTERNS (pattern-based whitelisting for vocab extraction)
    // ==========================================================================
    // These patterns match valid words that should NOT be flagged as suspicious
    // ==========================================================================
    
    // Scottish/Irish names: McDonald, MacArthur, McIntyre, etc.
    static ref WHITELIST_MC_NAMES: Regex = Regex::new(r"^(?i)M[ac]c?[A-Z][a-z]+$").unwrap();
    
    // British -ise spelling variants (vs American -ize)
    static ref WHITELIST_BRITISH_ISE: Regex = Regex::new(r"(?i)^[a-z]+is(e|ed|es|ing|ation|ations)$").unwrap();
    
    // Chemical formulas - curated list of common compounds
    // Pre-WWI era would have known these classical/inorganic compounds
    static ref CHEMICAL_FORMULAS: std::collections::HashSet<&'static str> = {
        let formulas = [
            // Water and common oxides
            "H2O", "CO2", "CO", "NO", "NO2", "SO2", "SO3",
            // Common acids
            "HCl", "HBr", "HI", "HF", "HNO3", "H2SO4", "H3PO4", "H2CO3", "HCN",
            // Common bases
            "NaOH", "KOH", "NH3", "NH4OH", "Ca(OH)2", "Mg(OH)2", "Ba(OH)2",
            // Salts - chlorides
            "NaCl", "KCl", "CaCl2", "MgCl2", "ZnCl2", "FeCl2", "FeCl3", "HgCl2", "BaCl2", "NH4Cl",
            // Salts - sulfates
            "Na2SO4", "K2SO4", "CaSO4", "MgSO4", "ZnSO4", "CuSO4", "FeSO4", "BaSO4", "Al2(SO4)3",
            // Salts - nitrates
            "NaNO3", "KNO3", "AgNO3", "Ca(NO3)2", "Pb(NO3)2", "Cu(NO3)2", "Zn(NO3)2",
            // Salts - carbonates
            "Na2CO3", "NaHCO3", "K2CO3", "CaCO3", "MgCO3", "BaCO3", "ZnCO3", "PbCO3",
            // Oxides
            "Na2O", "K2O", "CaO", "MgO", "ZnO", "CuO", "Cu2O", "Fe2O3", "Fe3O4", "FeO",
            "Al2O3", "SiO2", "PbO", "PbO2", "Pb3O4", "MnO2", "HgO", "Ag2O", "BaO",
            // Other common compounds
            "CaF2", "NaF", "KI", "NaBr", "AgCl", "AgBr", "AgI",
            "ZnS", "PbS", "CuS", "FeS", "FeS2", "H2S",
            "PCl3", "PCl5", "POCl3", "CCl4", "CHCl3",
            // Organic basics (period-appropriate)
            "CH4", "C2H6", "C2H4", "C2H2", "C6H6", "CH3OH", "C2H5OH", "HCHO", "CH3COOH",
            // Element symbols (when used alone in chemical context)
            "Na", "K", "Ca", "Mg", "Fe", "Cu", "Zn", "Ag", "Au", "Pb", "Hg", "Sn",
            "Al", "Si", "P", "S", "Cl", "Br", "I", "N", "O", "H", "C", "Ba", "Mn",
        ];
        formulas.iter().cloned().collect()
    };
    
    // Roman numeral pattern (skip these - they're period-appropriate)
    static ref ROMAN_NUMERAL_PATTERN: Regex = Regex::new(
        r"(?i)^[IVXLCDM]+$"
    ).unwrap();
}

/// Check if a word looks suspicious (likely OCR error)
fn check_suspicious(word: &str) -> Option<&'static str> {
    for (pattern, reason) in SUSPICIOUS_PATTERNS.iter() {
        if pattern.is_match(word) {
            // If dictionaries are loaded, check if word is known
            // Known words are NOT suspicious even if they match patterns
            if dictionary::dictionaries_loaded() && dictionary::is_known_word(word) {
                return None;
            }
            return Some(reason);
        }
    }
    None
}

/// Extract context around a position in text (UTF-8 safe)
fn extract_context(text: &str, start: usize, end: usize, context_chars: usize) -> String {
    let text_len = text.len();
    
    // Find start position, ensuring we land on a char boundary
    let mut ctx_start = start.saturating_sub(context_chars);
    // Move forward to a valid char boundary
    while ctx_start < text_len && !text.is_char_boundary(ctx_start) {
        ctx_start += 1;
    }
    // Try to expand to word boundary (only if ASCII)
    while ctx_start > 0 && text.is_char_boundary(ctx_start - 1) {
        let prev_byte = text.as_bytes()[ctx_start - 1];
        if prev_byte.is_ascii_alphanumeric() {
            ctx_start -= 1;
        } else {
            break;
        }
    }
    
    // Find end position, ensuring we land on a char boundary
    let mut ctx_end = (end + context_chars).min(text_len);
    // Move forward to a valid char boundary
    while ctx_end < text_len && !text.is_char_boundary(ctx_end) {
        ctx_end += 1;
    }
    // Try to expand to word boundary (only if ASCII)
    while ctx_end < text_len && text.is_char_boundary(ctx_end) {
        let curr_byte = text.as_bytes()[ctx_end];
        if curr_byte.is_ascii_alphanumeric() {
            ctx_end += 1;
        } else {
            break;
        }
    }
    
    // Final safety check
    if ctx_start >= ctx_end || ctx_start >= text_len {
        return String::new();
    }
    
    let mut context = String::new();
    if ctx_start > 0 {
        context.push_str("...");
    }
    context.push_str(&text[ctx_start..ctx_end]);
    if ctx_end < text_len {
        context.push_str("...");
    }
    
    // Normalize whitespace
    context.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Word occurrence data returned from Rust
#[pyclass]
#[derive(Clone)]
struct WordInfo {
    #[pyo3(get)]
    word: String,
    #[pyo3(get)]
    word_lower: String,
    #[pyo3(get)]
    is_capitalized: bool,
    #[pyo3(get)]
    is_suspicious: bool,
    #[pyo3(get)]
    suspicious_reason: String,
    #[pyo3(get)]
    context: String,
}

/// Extract vocabulary from a file
/// Returns: (word_count, list of WordInfo for unique words with first context)
#[pyfunction]
fn extract_vocab_from_file(
    file_path: String,
    context_chars: usize,
) -> PyResult<(u64, Vec<WordInfo>)> {
    use std::collections::HashMap;
    use std::fs;
    
    let content = fs::read_to_string(&file_path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Failed to read {}: {}", file_path, e)))?;
    
    let mut word_counts: HashMap<String, WordInfo> = HashMap::new();
    let mut total_words: u64 = 0;
    
    for cap in WORD_PATTERN.find_iter(&content) {
        let word = cap.as_str();
        let word_lower = word.to_lowercase();
        
        // Skip common words, short words (<3 chars), whitelisted words, and Roman numerals
        if word.len() < 3 || SKIP_WORDS.contains(word_lower.as_str()) || is_whitelisted(&word_lower) || ROMAN_NUMERAL_PATTERN.is_match(word) {
            continue;
        }
        
        // Skip ANY word that's in the dictionary - we only want unknown/suspicious candidates
        if dictionary::dictionaries_loaded() && dictionary::is_known_word(&word_lower) {
            continue;
        }
        
        total_words += 1;
        
        // Get or create entry
        if !word_counts.contains_key(&word_lower) {
            let is_cap = word.chars().next().map(|c| c.is_uppercase()).unwrap_or(false);
            let suspicious = check_suspicious(word);
            let context = extract_context(&content, cap.start(), cap.end(), context_chars);
            
            word_counts.insert(word_lower.clone(), WordInfo {
                word: word.to_string(),
                word_lower: word_lower.clone(),
                is_capitalized: word.chars().next().map(|c| c.is_uppercase()).unwrap_or(false),
                is_suspicious: suspicious.is_some(),
                suspicious_reason: suspicious.unwrap_or("").to_string(),
                context,
            });
        } else {
            // Update: prefer capitalized form
            let entry = word_counts.get_mut(&word_lower).unwrap();
            if word.chars().next().map(|c| c.is_uppercase()).unwrap_or(false) {
                entry.is_capitalized = true;
                if !entry.word.chars().next().map(|c| c.is_uppercase()).unwrap_or(false) {
                    entry.word = word.to_string();
                }
            }
        }
    }
    
    let results: Vec<WordInfo> = word_counts.into_values().collect();
    Ok((total_words, results))
}

/// Batch extract vocabulary from multiple files
/// Returns: (total_word_count, HashMap<word_lower, (word, count, is_cap, is_suspicious, reason, context)>)
#[pyfunction]
fn extract_vocab_batch(
    file_paths: Vec<String>,
    context_chars: usize,
) -> PyResult<(u64, std::collections::HashMap<String, (String, u64, bool, bool, String, String)>)> {
    use std::collections::HashMap;
    use std::fs;
    
    let mut global_counts: HashMap<String, (String, u64, bool, bool, String, String)> = HashMap::new();
    let mut total_words: u64 = 0;
    
    for file_path in file_paths {
        let content = match fs::read_to_string(&file_path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        
        for cap in WORD_PATTERN.find_iter(&content) {
            let word = cap.as_str();
            let word_lower = word.to_lowercase();
            
            // Skip common words, short words (<3 chars), whitelisted words, and Roman numerals
            if word.len() < 3 || SKIP_WORDS.contains(word_lower.as_str()) || is_whitelisted(&word_lower) || ROMAN_NUMERAL_PATTERN.is_match(word) {
                continue;
            }
            
            // Skip ANY word (capitalized or not) that's in the dictionary
            // We only want unknown/suspicious words as candidates
            if dictionary::dictionaries_loaded() && dictionary::is_known_word(&word_lower) {
                continue;
            }
            
            total_words += 1;
            
            let is_cap = word.chars().next().map(|c| c.is_uppercase()).unwrap_or(false);
            
            if let Some(entry) = global_counts.get_mut(&word_lower) {
                entry.1 += 1;  // Increment count
                if is_cap {
                    entry.2 = true;  // Mark as seen capitalized
                    if !entry.0.chars().next().map(|c| c.is_uppercase()).unwrap_or(false) {
                        entry.0 = word.to_string();  // Prefer capitalized form
                    }
                }
            } else {
                let suspicious = check_suspicious(word);
                let context = extract_context(&content, cap.start(), cap.end(), context_chars);
                
                global_counts.insert(word_lower.clone(), (
                    word.to_string(),
                    1,
                    is_cap,
                    suspicious.is_some(),
                    suspicious.unwrap_or("").to_string(),
                    context,
                ));
            }
        }
    }
    
    Ok((total_words, global_counts))
}

// =============================================================================
// DOCUMENT TRIAGE MODULE
// =============================================================================
// Fast heuristic-based document classification to filter out problematic content
// BEFORE running expensive OCR cleanup. Identifies:
// - Low quality scans (low alpha ratio, fragmented text)
// - Multicolumn content (newspapers with column mixing)
// - Catalog-like content (lists, indexes)

/// Triage result for a single document
#[pyclass]
#[derive(Clone)]
pub struct TriageResult {
    #[pyo3(get)]
    pub path: String,
    #[pyo3(get)]
    pub action: String,  // "pass", "quarantine", "reject"
    #[pyo3(get)]
    pub problems: Vec<String>,  // ["multicolumn", "low_alpha", "fragmented", "catalog_like"]
    #[pyo3(get)]
    pub alpha_ratio: f64,
    #[pyo3(get)]
    pub line_length_cv: f64,
    #[pyo3(get)]
    pub mean_words_per_line: f64,
    #[pyo3(get)]
    pub fragment_ratio: f64,
    #[pyo3(get)]
    pub list_pattern_ratio: f64,
    #[pyo3(get)]
    pub line_count: usize,
    #[pyo3(get)]
    pub char_count: usize,
}

#[pymethods]
impl TriageResult {
    fn to_dict(&self) -> std::collections::HashMap<String, pyo3::PyObject> {
        Python::with_gil(|py| {
            let mut map = std::collections::HashMap::new();
            map.insert("path".to_string(), self.path.clone().into_pyobject(py).unwrap().into_any().unbind());
            map.insert("action".to_string(), self.action.clone().into_pyobject(py).unwrap().into_any().unbind());
            map.insert("problems".to_string(), self.problems.clone().into_pyobject(py).unwrap().into_any().unbind());
            map.insert("alpha_ratio".to_string(), self.alpha_ratio.into_pyobject(py).unwrap().into_any().unbind());
            map.insert("line_length_cv".to_string(), self.line_length_cv.into_pyobject(py).unwrap().into_any().unbind());
            map.insert("mean_words_per_line".to_string(), self.mean_words_per_line.into_pyobject(py).unwrap().into_any().unbind());
            map.insert("fragment_ratio".to_string(), self.fragment_ratio.into_pyobject(py).unwrap().into_any().unbind());
            map.insert("list_pattern_ratio".to_string(), self.list_pattern_ratio.into_pyobject(py).unwrap().into_any().unbind());
            map.insert("line_count".to_string(), self.line_count.into_pyobject(py).unwrap().into_any().unbind());
            map.insert("char_count".to_string(), self.char_count.into_pyobject(py).unwrap().into_any().unbind());
            map
        })
    }
}

/// Thresholds for triage decisions (calibrated from corpus sampling)
struct TriageThresholds {
    // REJECT thresholds
    min_alpha_ratio: f64,           // Below this = garbage scan
    max_fragment_for_reject: f64,   // Combined with low words/line = reject
    min_words_per_line_reject: f64, // Combined with high fragment = reject
    
    // QUARANTINE thresholds (multicolumn)
    min_cv_for_multicolumn: f64,    // High variance suggests column mixing
    min_fragment_for_multicolumn: f64, // Combined with CV
    
    // QUARANTINE thresholds (catalog-like)
    min_list_pattern_ratio: f64,    // High list patterns = catalog/index
}

impl Default for TriageThresholds {
    fn default() -> Self {
        Self {
            // REJECT: garbage scans, photo albums
            min_alpha_ratio: 0.45,
            max_fragment_for_reject: 0.50,
            min_words_per_line_reject: 2.5,
            
            // QUARANTINE: multicolumn (newspapers)
            min_cv_for_multicolumn: 0.50,
            min_fragment_for_multicolumn: 0.25,
            
            // QUARANTINE: catalog-like content
            min_list_pattern_ratio: 0.15,
        }
    }
}

lazy_static! {
    static ref LIST_PATTERN: Regex = Regex::new(r"^\s*(\d+[\.\):\-]|\*|\-|•|[a-z][\.\)])").unwrap();
}

/// Compute triage signals from text content
fn compute_triage_signals(text: &str) -> (f64, f64, f64, f64, f64, usize, usize) {
    let char_count = text.len();
    if char_count == 0 {
        return (0.0, 0.0, 0.0, 1.0, 0.0, 0, 0);
    }
    
    // Alpha ratio
    let alpha_count = text.chars().filter(|c| c.is_alphabetic()).count();
    let alpha_ratio = alpha_count as f64 / char_count as f64;
    
    // Line-based signals
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    let line_count = lines.len();
    
    if line_count < 5 {
        // Too short to analyze meaningfully
        return (alpha_ratio, 0.0, 0.0, 1.0, 0.0, line_count, char_count);
    }
    
    // Line lengths for CV calculation
    let lengths: Vec<f64> = lines.iter().map(|l| l.len() as f64).collect();
    let mean_len: f64 = lengths.iter().sum::<f64>() / lengths.len() as f64;
    
    let line_length_cv = if mean_len > 0.0 && lengths.len() > 1 {
        let variance: f64 = lengths.iter()
            .map(|&x| (x - mean_len).powi(2))
            .sum::<f64>() / (lengths.len() - 1) as f64;
        variance.sqrt() / mean_len
    } else {
        0.0
    };
    
    // Words per line
    let word_counts: Vec<usize> = lines.iter()
        .map(|l| l.split_whitespace().count())
        .collect();
    let total_words: usize = word_counts.iter().sum();
    let mean_words_per_line = total_words as f64 / line_count as f64;
    
    // Fragment ratio (lines with < 3 words)
    let fragment_count = word_counts.iter().filter(|&&wc| wc < 3).count();
    let fragment_ratio = fragment_count as f64 / line_count as f64;
    
    // List pattern ratio
    let list_count = lines.iter()
        .filter(|l| LIST_PATTERN.is_match(l))
        .count();
    let list_pattern_ratio = list_count as f64 / line_count as f64;
    
    (alpha_ratio, line_length_cv, mean_words_per_line, fragment_ratio, 
     list_pattern_ratio, line_count, char_count)
}

/// Determine triage action and problems from signals
fn determine_triage(
    alpha_ratio: f64,
    line_length_cv: f64,
    mean_words_per_line: f64,
    fragment_ratio: f64,
    list_pattern_ratio: f64,
    line_count: usize,
) -> (String, Vec<String>) {
    let thresholds = TriageThresholds::default();
    let mut problems = Vec::new();
    
    // Check for REJECT conditions
    if alpha_ratio < thresholds.min_alpha_ratio {
        problems.push("low_alpha".to_string());
    }
    
    if mean_words_per_line < thresholds.min_words_per_line_reject 
        && fragment_ratio > thresholds.max_fragment_for_reject {
        problems.push("fragmented".to_string());
    }
    
    // Check for QUARANTINE conditions
    if line_length_cv > thresholds.min_cv_for_multicolumn 
        && fragment_ratio > thresholds.min_fragment_for_multicolumn {
        problems.push("multicolumn".to_string());
    }
    
    if list_pattern_ratio > thresholds.min_list_pattern_ratio {
        problems.push("catalog_like".to_string());
    }
    
    // Determine action
    let action = if problems.contains(&"low_alpha".to_string()) 
        || problems.contains(&"fragmented".to_string()) {
        "reject".to_string()
    } else if problems.contains(&"multicolumn".to_string()) 
        || problems.contains(&"catalog_like".to_string()) {
        "quarantine".to_string()
    } else if line_count < 5 {
        // Too short to evaluate - quarantine for manual review
        problems.push("too_short".to_string());
        "quarantine".to_string()
    } else {
        "pass".to_string()
    };
    
    (action, problems)
}

/// Triage a text string
#[pyfunction]
#[pyo3(signature = (text, path = ""))]
fn triage_text(text: &str, path: &str) -> PyResult<TriageResult> {
    let (alpha_ratio, line_length_cv, mean_words_per_line, fragment_ratio,
         list_pattern_ratio, line_count, char_count) = compute_triage_signals(text);
    
    let (action, problems) = determine_triage(
        alpha_ratio, line_length_cv, mean_words_per_line, 
        fragment_ratio, list_pattern_ratio, line_count
    );
    
    Ok(TriageResult {
        path: path.to_string(),
        action,
        problems,
        alpha_ratio,
        line_length_cv,
        mean_words_per_line,
        fragment_ratio,
        list_pattern_ratio,
        line_count,
        char_count,
    })
}

/// Triage a file
#[pyfunction]
fn triage_file(path: &str) -> PyResult<TriageResult> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(
            format!("Failed to read {}: {}", path, e)
        ))?;
    
    triage_text(&content, path)
}

/// Triage multiple files in batch (parallel processing)
#[pyfunction]
fn triage_batch(paths: Vec<String>) -> PyResult<Vec<TriageResult>> {
    use std::sync::{Arc, Mutex};
    use std::thread;
    
    let results = Arc::new(Mutex::new(Vec::with_capacity(paths.len())));
    let paths = Arc::new(paths);
    
    // Use available parallelism, capped at 8 threads
    let num_threads = std::thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(4);
    
    let chunk_size = (paths.len() + num_threads - 1) / num_threads;
    let mut handles = vec![];
    
    for chunk_idx in 0..num_threads {
        let paths = Arc::clone(&paths);
        let results = Arc::clone(&results);
        let start = chunk_idx * chunk_size;
        let end = (start + chunk_size).min(paths.len());
        
        if start >= paths.len() {
            break;
        }
        
        let handle = thread::spawn(move || {
            let mut local_results = Vec::new();
            
            for i in start..end {
                let path = &paths[i];
                match std::fs::read_to_string(path) {
                    Ok(content) => {
                        let (alpha_ratio, line_length_cv, mean_words_per_line, 
                             fragment_ratio, list_pattern_ratio, line_count, char_count) 
                            = compute_triage_signals(&content);
                        
                        let (action, problems) = determine_triage(
                            alpha_ratio, line_length_cv, mean_words_per_line,
                            fragment_ratio, list_pattern_ratio, line_count
                        );
                        
                        local_results.push(TriageResult {
                            path: path.clone(),
                            action,
                            problems,
                            alpha_ratio,
                            line_length_cv,
                            mean_words_per_line,
                            fragment_ratio,
                            list_pattern_ratio,
                            line_count,
                            char_count,
                        });
                    }
                    Err(e) => {
                        // File read error - mark as reject with error
                        local_results.push(TriageResult {
                            path: path.clone(),
                            action: "reject".to_string(),
                            problems: vec![format!("read_error: {}", e)],
                            alpha_ratio: 0.0,
                            line_length_cv: 0.0,
                            mean_words_per_line: 0.0,
                            fragment_ratio: 0.0,
                            list_pattern_ratio: 0.0,
                            line_count: 0,
                            char_count: 0,
                        });
                    }
                }
            }
            
            let mut results = results.lock().unwrap();
            results.extend(local_results);
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Thread panicked")
        })?;
    }
    
    let results = Arc::try_unwrap(results)
        .map_err(|_| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to unwrap results"))?
        .into_inner()
        .map_err(|_| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Mutex poisoned"))?;
    
    Ok(results)
}

// =============================================================================
// Language Detection (using whatlang)
// =============================================================================

/// Result of language detection
#[pyclass]
#[derive(Clone)]
pub struct LangDetectResult {
    #[pyo3(get)]
    pub is_english: bool,
    #[pyo3(get)]
    pub detected_lang: String,
    #[pyo3(get)]
    pub confidence: f64,
}

/// Detect if text is English using whatlang
/// Returns (is_english, detected_language_code, confidence)
#[pyfunction]
fn detect_language(text: &str, confidence_threshold: Option<f64>) -> LangDetectResult {
    let threshold = confidence_threshold.unwrap_or(0.5);
    
    // Use first 10k chars for speed
    let sample: String = text.chars().take(10000).collect();
    
    if sample.len() < 20 {
        // Too short to determine, assume English
        return LangDetectResult {
            is_english: true,
            detected_lang: "unknown".to_string(),
            confidence: 0.0,
        };
    }
    
    match detect(&sample) {
        Some(info) => {
            let is_english = info.lang() == Lang::Eng && info.confidence() >= threshold;
            LangDetectResult {
                is_english,
                detected_lang: format!("{:?}", info.lang()).to_lowercase(),
                confidence: info.confidence(),
            }
        }
        None => {
            // Detection failed, assume English to avoid blocking
            LangDetectResult {
                is_english: true,
                detected_lang: "unknown".to_string(),
                confidence: 0.0,
            }
        }
    }
}

/// Detect language from a file
#[pyfunction]
fn detect_language_file(path: &str, confidence_threshold: Option<f64>) -> PyResult<LangDetectResult> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read file: {}", e)))?;
    Ok(detect_language(&content, confidence_threshold))
}

// =============================================================================
// Unicode Normalization (ftfy-equivalent)
// =============================================================================

/// Fix common Unicode issues in text
/// - Normalizes to NFC form (canonical composition)
/// - Fixes common mojibake patterns
/// - Normalizes whitespace characters
#[pyfunction]
fn fix_unicode(text: &str) -> String {
    // Step 1: NFC normalization (canonical decomposition + canonical composition)
    let normalized: String = text.nfc().collect();
    
    // Step 2: Fix common mojibake patterns
    // These are UTF-8 bytes misinterpreted as Latin-1/Windows-1252
    let fixed = fix_mojibake(&normalized);
    
    // Step 3: Normalize various Unicode whitespace to ASCII equivalents
    let fixed = normalize_unicode_whitespace(&fixed);
    
    // Step 4: Fix common HTML entities that might have been double-encoded
    let fixed = fix_html_entities(&fixed);
    
    fixed
}

/// Fix common mojibake patterns (UTF-8 misread as Latin-1)
fn fix_mojibake(text: &str) -> String {
    lazy_static! {
        static ref MOJIBAKE_PATTERNS: Vec<(&'static str, &'static str)> = vec![
            // Common UTF-8 -> Latin-1 mojibake (using escape sequences)
            // Accented vowels
            ("\u{00C3}\u{00A1}", "\u{00E1}"),  // Ã¡ -> á
            ("\u{00C3}\u{00A9}", "\u{00E9}"),  // Ã© -> é
            ("\u{00C3}\u{00AD}", "\u{00ED}"),  // Ã­ -> í
            ("\u{00C3}\u{00B3}", "\u{00F3}"),  // Ã³ -> ó
            ("\u{00C3}\u{00BA}", "\u{00FA}"),  // Ãº -> ú
            ("\u{00C3}\u{00B1}", "\u{00F1}"),  // Ã± -> ñ
            ("\u{00C3}\u{00BC}", "\u{00FC}"),  // Ã¼ -> ü
            ("\u{00C3}\u{00B6}", "\u{00F6}"),  // Ã¶ -> ö
            ("\u{00C3}\u{00A4}", "\u{00E4}"),  // Ã¤ -> ä
            ("\u{00C3}\u{00A8}", "\u{00E8}"),  // Ã¨ -> è
            ("\u{00C3}\u{00A0}", "\u{00E0}"),  // Ã  -> à
            ("\u{00C3}\u{00A2}", "\u{00E2}"),  // Ã¢ -> â
            ("\u{00C3}\u{00AA}", "\u{00EA}"),  // Ãª -> ê
            ("\u{00C3}\u{00AE}", "\u{00EE}"),  // Ã® -> î
            ("\u{00C3}\u{00B4}", "\u{00F4}"),  // Ã´ -> ô
            ("\u{00C3}\u{00BB}", "\u{00FB}"),  // Ã» -> û
            ("\u{00C3}\u{00A7}", "\u{00E7}"),  // Ã§ -> ç
            ("\u{00C3}\u{00BF}", "\u{00FF}"),  // Ã¿ -> ÿ
            ("\u{00C3}\u{00AF}", "\u{00EF}"),  // Ã¯ -> ï
            ("\u{00C3}\u{00B8}", "\u{00F8}"),  // Ã¸ -> ø
            ("\u{00C3}\u{00A6}", "\u{00E6}"),  // Ã¦ -> æ
            ("\u{00C3}\u{00B0}", "\u{00F0}"),  // Ã° -> ð
            ("\u{00C3}\u{00BD}", "\u{00FD}"),  // Ã½ -> ý
            // Curly quotes mojibake
            ("\u{00E2}\u{20AC}\u{0153}", "\""),  // â€œ -> "
            ("\u{00E2}\u{20AC}\u{009D}", "\""),  // â€ -> "
            ("\u{00E2}\u{20AC}\u{02DC}", "'"),   // â€˜ -> '
            ("\u{00E2}\u{20AC}\u{2122}", "'"),   // â€™ -> '
            // Em/en dash mojibake
            ("\u{00E2}\u{20AC}\u{201C}", "\u{2014}"),  // â€" -> —
            ("\u{00E2}\u{20AC}\u{201D}", "\u{2013}"),  // â€" -> –
            // Ellipsis
            ("\u{00E2}\u{20AC}\u{00A6}", "\u{2026}"),  // â€¦ -> …
            // Non-breaking space mojibake
            ("\u{00C2}\u{00A0}", " "),  // Â  -> space
        ];
    }
    
    let mut result = text.to_string();
    for (pattern, replacement) in MOJIBAKE_PATTERNS.iter() {
        result = result.replace(pattern, replacement);
    }
    result
}

/// Normalize various Unicode whitespace characters
fn normalize_unicode_whitespace(text: &str) -> String {
    lazy_static! {
        static ref WHITESPACE_MAP: Vec<(char, Option<char>)> = vec![
            ('\u{00A0}', Some(' ')),  // Non-breaking space
            ('\u{2000}', Some(' ')),  // En quad
            ('\u{2001}', Some(' ')),  // Em quad
            ('\u{2002}', Some(' ')),  // En space
            ('\u{2003}', Some(' ')),  // Em space
            ('\u{2004}', Some(' ')),  // Three-per-em space
            ('\u{2005}', Some(' ')),  // Four-per-em space
            ('\u{2006}', Some(' ')),  // Six-per-em space
            ('\u{2007}', Some(' ')),  // Figure space
            ('\u{2008}', Some(' ')),  // Punctuation space
            ('\u{2009}', Some(' ')),  // Thin space
            ('\u{200A}', Some(' ')),  // Hair space
            ('\u{202F}', Some(' ')),  // Narrow no-break space
            ('\u{205F}', Some(' ')),  // Medium mathematical space
            ('\u{3000}', Some(' ')),  // Ideographic space
            ('\u{FEFF}', None),       // BOM / zero-width no-break space (remove)
        ];
    }
    
    let mut result = String::with_capacity(text.len());
    for c in text.chars() {
        let mut found = false;
        for (from, to) in WHITESPACE_MAP.iter() {
            if c == *from {
                if let Some(replacement) = to {
                    result.push(*replacement);
                }
                // If None, we skip (remove the character)
                found = true;
                break;
            }
        }
        if !found {
            result.push(c);
        }
    }
    result
}

/// Fix common HTML entities
fn fix_html_entities(text: &str) -> String {
    lazy_static! {
        static ref HTML_ENTITIES: Vec<(&'static str, &'static str)> = vec![
            ("&amp;", "&"),
            ("&lt;", "<"),
            ("&gt;", ">"),
            ("&quot;", "\""),
            ("&apos;", "'"),
            ("&#39;", "'"),
            ("&nbsp;", " "),
            // Double-encoded
            ("&amp;amp;", "&"),
            ("&amp;lt;", "<"),
            ("&amp;gt;", ">"),
        ];
    }
    
    let mut result = text.to_string();
    for (entity, replacement) in HTML_ENTITIES.iter() {
        result = result.replace(entity, replacement);
    }
    result
}

/// Fix unicode issues in a file and write to output
#[pyfunction]
fn fix_unicode_file(input_path: &str, output_path: &str) -> PyResult<bool> {
    let content = std::fs::read_to_string(input_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read file: {}", e)))?;
    
    let fixed = fix_unicode(&content);
    let was_modified = fixed != content;
    
    std::fs::write(output_path, &fixed)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to write file: {}", e)))?;
    
    Ok(was_modified)
}

// =============================================================================
// Combined preprocessing: unicode fix + language detection
// =============================================================================

/// Result of preprocessing a file
#[pyclass]
#[derive(Clone)]
pub struct PreprocessResult {
    #[pyo3(get)]
    pub is_english: bool,
    #[pyo3(get)]
    pub detected_lang: String,
    #[pyo3(get)]
    pub lang_confidence: f64,
    #[pyo3(get)]
    pub unicode_was_fixed: bool,
}

/// Preprocess text: fix unicode and detect language
#[pyfunction]
fn preprocess_text(text: &str, confidence_threshold: Option<f64>) -> (String, PreprocessResult) {
    let fixed = fix_unicode(text);
    let unicode_was_fixed = fixed != text;
    
    let lang_result = detect_language(&fixed, confidence_threshold);
    
    let result = PreprocessResult {
        is_english: lang_result.is_english,
        detected_lang: lang_result.detected_lang,
        lang_confidence: lang_result.confidence,
        unicode_was_fixed,
    };
    
    (fixed, result)
}

/// Preprocess a file: fix unicode, detect language, optionally write output
/// Returns PreprocessResult. If output_path is provided and text is English, writes fixed content.
#[pyfunction]
fn preprocess_file(
    input_path: &str, 
    output_path: Option<&str>,
    confidence_threshold: Option<f64>
) -> PyResult<PreprocessResult> {
    let content = std::fs::read_to_string(input_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read file: {}", e)))?;
    
    let (fixed, result) = preprocess_text(&content, confidence_threshold);
    
    // Only write if English and output path provided
    if let Some(out_path) = output_path {
        if result.is_english {
            if let Some(parent) = std::path::Path::new(out_path).parent() {
                std::fs::create_dir_all(parent).ok();
            }
            std::fs::write(out_path, &fixed)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to write file: {}", e)))?;
        }
    }
    
    Ok(result)
}

// Python bindings for dictionary functions
#[pyfunction]
fn init_dictionaries(dict_dir: &str) -> bool {
    dictionary::init_dictionaries(dict_dir)
}

#[pyfunction]
fn is_known_word(word: &str) -> bool {
    dictionary::is_known_word(word)
}

#[pyfunction]
fn word_languages(word: &str) -> Vec<&'static str> {
    dictionary::word_languages(word)
}

#[pyfunction]
fn dictionaries_loaded() -> bool {
    dictionary::dictionaries_loaded()
}

// =============================================================================
// BOILERPLATE STRIPPING
// =============================================================================
// Removes digitization boilerplate from Google Books, Internet Archive, etc.
// Should run BEFORE OCR cleanup since boilerplate is modern inserted text.
// Logs what was stripped for audit purposes.

/// A region that was stripped from the document
#[pyclass]
#[derive(Clone)]
pub struct StrippedRegion {
    #[pyo3(get)]
    pub category: String,
    #[pyo3(get)]
    pub pattern_name: String,
    #[pyo3(get)]
    pub start_line: usize,
    #[pyo3(get)]
    pub end_line: usize,
    #[pyo3(get)]
    pub char_count: usize,
}

/// Result of boilerplate stripping
#[pyclass]
#[derive(Clone)]
pub struct BoilerplateResult {
    #[pyo3(get)]
    pub text: String,
    #[pyo3(get)]
    pub stripped_regions: Vec<StrippedRegion>,
    #[pyo3(get)]
    pub total_chars_stripped: usize,
}

lazy_static! {
    // =========================================================================
    // BOILERPLATE PATTERNS
    // =========================================================================
    // Each pattern: (name, category, regex, search_location)
    // search_location: "start" = first 15KB, "end" = last 10KB, "any" = full text
    // Patterns are OCR-tolerant to handle damaged text
    // =========================================================================
    
    // Google Books - main disclaimer block
    // Matches from "This is a digital copy" through the end of the boilerplate
    // Captures the full block including "You can search through..." and trailing URL
    // OCR variants: "qooqle", "VjOOQ", "GoOglc", etc.
    static ref GOOGLE_BOOKS_BLOCK: Regex = Regex::new(
        r"(?is)This\s+is\s+a\s+digital\s+copy\s+of\s+a\s+book\s+that\s+was\s+preserved.*?(?:search\s+through\s+the\s+full\s+text\s+of\s+this\s+book\s+on\s+the\s+web|Book\s+Search\s+helps\s+readers).*?(?:h?t?t?p?\s*:?\s*/?/?\s*books\s*\.\s*(?:google|qooqle|[VLC]j?OOQ\w*|GoOglc)\s*\.\s*com\s*/?\s*|\n\s*\n)"
    ).unwrap();
    
    // Google Books - shorter variant (mission statement)
    static ref GOOGLE_BOOKS_MISSION: Regex = Regex::new(
        r"(?is)'s\s+mission\s+is\s+to\s+organize\s+the\s+world's\s+information.*?Book\s+Search"
    ).unwrap();
    
    // Google watermark URL line (standalone)
    // OCR-tolerant: handles "jhttp", "littp", spaces around punctuation, etc.
    static ref GOOGLE_URL_LINE: Regex = Regex::new(
        r"(?im)^\s*(?:at\s+)?(?:[hjli]?https?|[hjli]ttp)\s*:\s*/?\s*/?\s*books\s*\.\s*(?:google|qooqle|[VLC]j?OOQ\w*|GoOglc)\s*\.\s*com\s*/?\s*$"
    ).unwrap();
    
    // Internet Archive header - "Digitized by the Archive" + URL
    static ref IA_HEADER_BLOCK: Regex = Regex::new(
        r"(?is)Digitized\s+(?:by\s+)?(?:the\s+)?(?:Internet\s+)?Archive\s*[\n\r].*?(?:https?://)?archive\.org/details/\S+"
    ).unwrap();
    
    // Internet Archive - simple one-liner
    static ref IA_DIGITIZED_LINE: Regex = Regex::new(
        r"(?im)^.*Digitized\s+(?:by\s+)?(?:the\s+)?(?:Internet\s+)?Archive.*$"
    ).unwrap();
    
    // Internet Archive URL line (clean)
    static ref IA_URL_LINE: Regex = Regex::new(
        r"(?im)^\s*(?:https?://)?(?:www\.)?archive\.org/details/\S+\s*$"
    ).unwrap();
    
    // Internet Archive URL line (OCR-damaged with spaces inserted)
    // Matches: "https ://arch i ve . o rg/detai Is/..." 
    static ref IA_URL_LINE_OCR: Regex = Regex::new(
        r"(?im)^.*https?\s*:\s*//\s*a\s*r\s*c\s*h\s*i\s*v\s*e\s*\.\s*o\s*r\s*g.*$"
    ).unwrap();
    
    // Microsoft digitization (you mentioned seeing OCR-damaged Microsoft attribution)
    static ref MICROSOFT_DIGITIZED: Regex = Regex::new(
        r"(?im)^.*(?:Digitized|Scanned)\s+(?:by\s+)?(?:the\s+)?(?:Microsoft|[Mm]icros[oa]ft|Mlcrosoft).*$"
    ).unwrap();
    
    // University library stamps (end of document)
    static ref UNIVERSITY_LIBRARY: Regex = Regex::new(
        r"(?is)(?:THE\s+)?UNIVERSITY\s+OF\s+\w+\s*[\n\r]+\s*(?:GRADUATE\s+)?LIBRARY"
    ).unwrap();
    
    // Leeds University Library stamp (start of document)
    // Pattern: LEEDS UNIVERSITY LIBRARY + Classmark block
    static ref LEEDS_LIBRARY: Regex = Regex::new(
        r"(?is)LEEDS\s+UNIVERSITY\s+LIBRARY\s*[\n\r]+\s*(?:Classmark:)?(?:.*[\n\r]+){0,10}(?:The\s+University\s+Library\s+Leeds|Medical\s+and\s+Dental\s+Library)"
    ).unwrap();
    
    // Library "DATE DUE" cards
    static ref DATE_DUE_CARD: Regex = Regex::new(
        r"(?is)DATE\s+DUE\s*[\n\r]+(?:.*[\n\r]+){0,15}"
    ).unwrap();
    
    // Library barcodes (pattern like "3 9015 030 7 4133")
    static ref LIBRARY_BARCODE: Regex = Regex::new(
        r"(?m)^\s*\d\s+\d{4}\s+\d{3}\s+\d+\s+\d+\s*$"
    ).unwrap();
    
    // "CIRCULATE CARD" or OCR variant "IITILATE CARD"
    static ref CIRCULATE_CARD: Regex = Regex::new(
        r"(?im)(?:CIRCULATE|IITILATE)\s+CAR[DK]"
    ).unwrap();
    
    // Yale/Harvard specific library stamps
    static ref YALE_LIBRARY: Regex = Regex::new(
        r"(?is)YALE\s+(?:MEDICAL\s+)?LIBRARY.*?(?:HISTORICAL\s+LIBRARY|Bequest\s+of\s+\w+)"
    ).unwrap();
    
    // HathiTrust digitization notice
    static ref HATHITRUST: Regex = Regex::new(
        r"(?is)(?:Generated|Digitized)\s+(?:by|for)\s+HathiTrust.*?(?:www\.hathitrust\.org|public\s+domain)"
    ).unwrap();
    
    // Generic digitization notice
    static ref GENERIC_DIGITIZED: Regex = Regex::new(
        r"(?im)^.*(?:This\s+book\s+was\s+)?[Dd]igitized\s+(?:by|from|at)\s+.*?(?:Library|Archive|University).*$"
    ).unwrap();
}

/// Find line number for a character position
fn char_to_line(text: &str, char_pos: usize) -> usize {
    text[..char_pos.min(text.len())].matches('\n').count() + 1
}

/// Internal function to strip boilerplate from text
fn strip_boilerplate_internal(text: &str) -> (String, Vec<StrippedRegion>) {
    let mut stripped_regions: Vec<StrippedRegion> = Vec::new();
    let mut regions_to_remove: Vec<(usize, usize, String, String)> = Vec::new(); // (start, end, category, pattern_name)
    
    let text_len = text.len();
    
    // Helper to find nearest valid UTF-8 char boundary at or before a byte index
    let floor_char_boundary = |idx: usize| -> usize {
        if idx >= text_len {
            return text_len;
        }
        // Walk backwards to find a char boundary
        let mut i = idx;
        while i > 0 && !text.is_char_boundary(i) {
            i -= 1;
        }
        i
    };
    
    // Search boundaries (adjusted to char boundaries)
    let start_boundary = floor_char_boundary(text_len.min(15000)); // First ~15KB for start patterns
    let end_boundary = floor_char_boundary(text_len.saturating_sub(10000)); // Last ~10KB for end patterns
    
    // Helper to add matches from a regex
    let mut add_matches = |regex: &Regex, category: &str, pattern_name: &str, search_start: usize, search_end: usize| {
        let safe_end = if text.is_char_boundary(search_end.min(text_len)) {
            search_end.min(text_len)
        } else {
            floor_char_boundary(search_end.min(text_len))
        };
        let search_text = &text[search_start..safe_end];
        for mat in regex.find_iter(search_text) {
            let abs_start = search_start + mat.start();
            let abs_end = search_start + mat.end();
            regions_to_remove.push((abs_start, abs_end, category.to_string(), pattern_name.to_string()));
        }
    };
    
    // START patterns (first 15KB)
    add_matches(&GOOGLE_BOOKS_BLOCK, "google_books", "google_books_disclaimer", 0, start_boundary);
    add_matches(&GOOGLE_BOOKS_MISSION, "google_books", "google_books_mission", 0, start_boundary);
    add_matches(&GOOGLE_URL_LINE, "google_books", "google_url_line", 0, start_boundary);
    add_matches(&IA_HEADER_BLOCK, "internet_archive", "ia_header_block", 0, start_boundary);
    add_matches(&IA_DIGITIZED_LINE, "internet_archive", "ia_digitized_line", 0, start_boundary);
    add_matches(&IA_URL_LINE, "internet_archive", "ia_url_line", 0, start_boundary);
    add_matches(&IA_URL_LINE_OCR, "internet_archive", "ia_url_line_ocr", 0, start_boundary);
    add_matches(&MICROSOFT_DIGITIZED, "microsoft", "microsoft_digitized", 0, start_boundary);
    add_matches(&YALE_LIBRARY, "library_stamp", "yale_library", 0, start_boundary);
    add_matches(&LEEDS_LIBRARY, "library_stamp", "leeds_library", 0, start_boundary);
    add_matches(&HATHITRUST, "hathitrust", "hathitrust", 0, start_boundary);
    add_matches(&GENERIC_DIGITIZED, "generic", "generic_digitized", 0, start_boundary);
    
    // END patterns (last 10KB)
    if end_boundary < text_len {
        add_matches(&UNIVERSITY_LIBRARY, "library_stamp", "university_library", end_boundary, text_len);
        add_matches(&DATE_DUE_CARD, "library_stamp", "date_due_card", end_boundary, text_len);
        add_matches(&LIBRARY_BARCODE, "library_stamp", "library_barcode", end_boundary, text_len);
        add_matches(&CIRCULATE_CARD, "library_stamp", "circulate_card", end_boundary, text_len);
    }
    
    // Sort regions by start position
    regions_to_remove.sort_by_key(|r| r.0);
    
    // KEY OPTIMIZATION: If boilerplate is detected in the first 15KB, extend strip region
    // back to document start. Rationale: anything before digitization notices (Google Books
    // disclaimer, "Digitized in 2016", etc.) is almost certainly OCR garbage from cover
    // pages, binding artifacts, or library stamps - not legitimate content.
    for region in regions_to_remove.iter_mut() {
        if region.0 < start_boundary && region.0 > 0 {
            // This boilerplate starts in the first 15KB - extend back to start
            region.0 = 0;
        }
    }
    
    // Re-sort after potential modifications
    regions_to_remove.sort_by_key(|r| r.0);
    
    // Merge overlapping regions
    let mut merged: Vec<(usize, usize, String, String)> = Vec::new();
    for region in regions_to_remove {
        if let Some(last) = merged.last_mut() {
            if region.0 <= last.1 {
                // Overlapping - extend the last region
                last.1 = last.1.max(region.1);
                // Keep the category of the larger match
                continue;
            }
        }
        merged.push(region);
    }
    
    // Build stripped text and record what was removed
    let mut result = String::with_capacity(text_len);
    let mut last_end = 0;
    
    for (start, end, category, pattern_name) in merged {
        // Add text before this region
        if start > last_end {
            result.push_str(&text[last_end..start]);
        }
        
        // Record the stripped region
        let start_line = char_to_line(text, start);
        let end_line = char_to_line(text, end);
        let char_count = end - start;
        
        stripped_regions.push(StrippedRegion {
            category,
            pattern_name,
            start_line,
            end_line,
            char_count,
        });
        
        last_end = end;
    }
    
    // Add remaining text after last region
    if last_end < text_len {
        result.push_str(&text[last_end..]);
    }
    
    // Trim leading/trailing whitespace that may have been exposed
    let trimmed = result.trim().to_string();
    
    (trimmed, stripped_regions)
}

/// Strip boilerplate from text
/// Returns BoilerplateResult with cleaned text and list of stripped regions
#[pyfunction]
fn strip_boilerplate(text: &str) -> BoilerplateResult {
    let (cleaned, regions) = strip_boilerplate_internal(text);
    let total_chars_stripped: usize = regions.iter().map(|r| r.char_count).sum();
    
    BoilerplateResult {
        text: cleaned,
        stripped_regions: regions,
        total_chars_stripped,
    }
}

/// Strip boilerplate from a file and optionally write to output
/// Returns BoilerplateResult
#[pyfunction]
#[pyo3(signature = (input_path, output_path=None))]
fn strip_boilerplate_file(input_path: &str, output_path: Option<&str>) -> PyResult<BoilerplateResult> {
    let content = std::fs::read_to_string(input_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read file: {}", e)))?;
    
    let (cleaned, regions) = strip_boilerplate_internal(&content);
    let total_chars_stripped: usize = regions.iter().map(|r| r.char_count).sum();
    
    // Write output if path provided
    if let Some(out_path) = output_path {
        if let Some(parent) = std::path::Path::new(out_path).parent() {
            std::fs::create_dir_all(parent).ok();
        }
        std::fs::write(out_path, &cleaned)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to write file: {}", e)))?;
    }
    
    Ok(BoilerplateResult {
        text: cleaned,
        stripped_regions: regions,
        total_chars_stripped,
    })
}

/// Batch strip boilerplate from files in a directory
/// Returns (files_processed, files_with_boilerplate, total_chars_stripped)
#[pyfunction]
fn strip_boilerplate_batch(input_dir: &str, output_dir: &str) -> PyResult<(u64, u64, u64)> {
    use std::fs;
    use std::path::Path;
    
    let input_path = Path::new(input_dir);
    let output_path = Path::new(output_dir);
    
    // Create output directory
    fs::create_dir_all(output_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to create output dir: {}", e)))?;
    
    let mut files_processed: u64 = 0;
    let mut files_with_boilerplate: u64 = 0;
    let mut total_chars_stripped: u64 = 0;
    
    let entries = fs::read_dir(input_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read input dir: {}", e)))?;
    
    for entry in entries {
        let entry = entry.map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read entry: {}", e)))?;
        let path = entry.path();
        
        // Only process .txt files
        if path.extension().map(|e| e == "txt").unwrap_or(false) {
            if let Some(filename) = path.file_name() {
                let output_file = output_path.join(filename);
                
                match fs::read_to_string(&path) {
                    Ok(content) => {
                        let (cleaned, regions) = strip_boilerplate_internal(&content);
                        
                        if let Err(e) = fs::write(&output_file, &cleaned) {
                            eprintln!("Warning: Failed to write {}: {}", output_file.display(), e);
                            continue;
                        }
                        
                        files_processed += 1;
                        if !regions.is_empty() {
                            files_with_boilerplate += 1;
                            total_chars_stripped += regions.iter().map(|r| r.char_count as u64).sum::<u64>();
                        }
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to read {}: {}", path.display(), e);
                    }
                }
            }
        }
    }
    
    Ok((files_processed, files_with_boilerplate, total_chars_stripped))
}

// =============================================================================
// LINE UNWRAPPING AND DEHYPHENATION
// =============================================================================
// Removes cosmetic line breaks while preserving paragraph structure.
// Handles hyphenated word breaks at line endings.
// Normalizes extra whitespace.

/// Result of line unwrapping
#[pyclass]
#[derive(Clone)]
pub struct UnwrapResult {
    #[pyo3(get)]
    pub text: String,
    #[pyo3(get)]
    pub lines_joined: u64,
    #[pyo3(get)]
    pub words_dehyphenated: u64,
    #[pyo3(get)]
    pub spaces_normalized: u64,
}

/// Check if a line appears to be a paragraph break or structural element
fn is_paragraph_boundary(line: &str, next_line: Option<&str>) -> bool {
    let trimmed = line.trim();
    
    // Empty or whitespace-only line = paragraph break
    if trimmed.is_empty() {
        return true;
    }
    
    // Line is just a page number (digits only, possibly with punctuation)
    if trimmed.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '-') && trimmed.len() <= 10 {
        return true;
    }
    
    // Very short lines that look like headers/titles (ALL CAPS or Title Case with few words)
    let words: Vec<&str> = trimmed.split_whitespace().collect();
    if words.len() <= 5 && trimmed.len() < 60 {
        // Check if ALL CAPS
        let alpha_chars: String = trimmed.chars().filter(|c| c.is_alphabetic()).collect();
        if !alpha_chars.is_empty() && alpha_chars.chars().all(|c| c.is_uppercase()) {
            return true;
        }
    }
    
    // Line ends with terminal punctuation AND next line starts with capital letter
    // This suggests a natural sentence/paragraph boundary
    if let Some(last_char) = trimmed.chars().last() {
        if matches!(last_char, '.' | '!' | '?' | ':' | '"' | '\'' | ')' | ']') {
            if let Some(next) = next_line {
                let next_trimmed = next.trim();
                if let Some(first_char) = next_trimmed.chars().find(|c| c.is_alphabetic()) {
                    if first_char.is_uppercase() {
                        return true;
                    }
                }
            }
        }
    }
    
    false
}

lazy_static! {
    // Pattern to match hyphenated line breaks: word-\n followed by lowercase continuation
    // Captures: (word before hyphen)(hyphen)(newline + optional space)(lowercase continuation)
    static ref HYPHEN_LINEBREAK: Regex = Regex::new(r"([a-zA-Z]{2,})-[ \t]*\r?\n[ \t]*([a-z])").unwrap();
    
    // ==========================================================================
    // FRAGMENT REJOINING PATTERN (single optimized regex)
    // ==========================================================================
    // Catches cases where a hyphen was OCR'd as a comma or space, leaving
    // orphaned suffixes like "accord, ing" or "judg ment"
    // Uses alternation for all suffixes in ONE regex scan (not 30 separate scans)
    // ==========================================================================
    
    static ref FRAGMENT_REJOIN: Regex = Regex::new(
        r"(?i)\b([a-z]{2,})[,\s]+\b(ings?|tions?|sions?|ments?|ness|ly|ed|ers?|est|fully?|less|[ai]ble|ity|ities|i?ous|[ae]nce|ively?)\b"
    ).unwrap();
}

/// Phase 1: Dehyphenate - rejoin words split across lines with hyphens
fn dehyphenate(text: &str) -> (String, u64) {
    let mut count: u64 = 0;
    let result = HYPHEN_LINEBREAK.replace_all(text, |caps: &regex::Captures| {
        count += 1;
        // Join word part + continuation letter (captures are 1-indexed)
        format!("{}{}", &caps[1], &caps[2])
    });
    (result.into_owned(), count)
}

/// Phase 1b: Rejoin fragments where hyphen was OCR'd as comma or space
/// e.g., "accord, ing" -> "according", "judg ment" -> "judgment"
fn rejoin_fragments(text: &str) -> (String, u64) {
    let mut count: u64 = 0;
    let result = FRAGMENT_REJOIN.replace_all(text, |caps: &regex::Captures| {
        count += 1;
        // caps[1] = word stem, caps[2] = suffix
        format!("{}{}", &caps[1], &caps[2])
    });
    (result.into_owned(), count)
}

/// Internal function to unwrap lines (multi-phase approach)
fn unwrap_lines_internal(text: &str) -> (String, u64, u64, u64) {
    // Phase 1a: Dehyphenate (rejoin hyphenated words across line breaks)
    let (dehyphenated, words_dehyphenated) = dehyphenate(text);
    
    // Phase 1b: Rejoin fragments (hyphen OCR'd as comma/space)
    let (rejoined, fragments_rejoined) = rejoin_fragments(&dehyphenated);
    let total_dehyphenated = words_dehyphenated + fragments_rejoined;
    
    // Phase 2: Join non-paragraph lines
    let mut result = String::with_capacity(rejoined.len());
    let mut lines_joined: u64 = 0;
    let mut spaces_normalized: u64 = 0;
    
    let lines: Vec<&str> = rejoined.lines().collect();
    
    for (i, line) in lines.iter().enumerate() {
        let next_line = lines.get(i + 1).copied();
        
        // Add the current line (trimmed)
        result.push_str(line.trim_end());
        
        // Decide whether to join with next line or preserve line break
        if let Some(next) = next_line {
            if is_paragraph_boundary(line, Some(next)) {
                // Preserve line break (paragraph boundary)
                result.push('\n');
            } else if next.trim().is_empty() {
                // Next line is blank - preserve
                result.push('\n');
            } else {
                // Join lines with a space
                result.push(' ');
                lines_joined += 1;
            }
        }
    }
    
    // Normalize multiple spaces to single space
    let mut normalized = String::with_capacity(result.len());
    let mut prev_space = false;
    let mut prev_newline = false;
    
    for c in result.chars() {
        if c == ' ' {
            if !prev_space && !prev_newline {
                normalized.push(c);
            } else if prev_space {
                spaces_normalized += 1;
            }
            prev_space = true;
            prev_newline = false;
        } else if c == '\n' {
            // Don't add space before newline, preserve newlines
            if prev_space {
                // Remove trailing space before newline
                normalized.pop();
            }
            normalized.push(c);
            prev_space = false;
            prev_newline = true;
        } else {
            normalized.push(c);
            prev_space = false;
            prev_newline = false;
        }
    }
    
    (normalized, lines_joined, total_dehyphenated, spaces_normalized)
}

/// Unwrap cosmetic line breaks while preserving paragraph structure
/// Returns UnwrapResult with text and statistics
#[pyfunction]
fn unwrap_lines(text: &str) -> UnwrapResult {
    let (text, lines_joined, words_dehyphenated, spaces_normalized) = unwrap_lines_internal(text);
    UnwrapResult {
        text,
        lines_joined,
        words_dehyphenated,
        spaces_normalized,
    }
}

/// Unwrap lines in a file and write to output
#[pyfunction]
fn unwrap_lines_file(input_path: &str, output_path: &str) -> PyResult<UnwrapResult> {
    let content = std::fs::read_to_string(input_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read file: {}", e)))?;
    
    let (text, lines_joined, words_dehyphenated, spaces_normalized) = unwrap_lines_internal(&content);
    
    // Ensure parent directory exists
    if let Some(parent) = std::path::Path::new(output_path).parent() {
        std::fs::create_dir_all(parent).ok();
    }
    
    std::fs::write(output_path, &text)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to write file: {}", e)))?;
    
    Ok(UnwrapResult {
        text,
        lines_joined,
        words_dehyphenated,
        spaces_normalized,
    })
}

/// Batch unwrap lines in multiple files
#[pyfunction]
fn unwrap_lines_batch(input_dir: &str, output_dir: &str) -> PyResult<(u64, u64, u64, u64)> {
    use std::fs;
    use std::path::Path;
    
    let input_path = Path::new(input_dir);
    let output_path = Path::new(output_dir);
    
    // Create output directory
    fs::create_dir_all(output_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to create output dir: {}", e)))?;
    
    let mut total_files: u64 = 0;
    let mut total_lines_joined: u64 = 0;
    let mut total_words_dehyphenated: u64 = 0;
    let mut total_spaces_normalized: u64 = 0;
    
    // Iterate over files in input directory
    let entries = fs::read_dir(input_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read input dir: {}", e)))?;
    
    for entry in entries {
        let entry = entry.map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read entry: {}", e)))?;
        let path = entry.path();
        
        // Only process .txt files
        if path.extension().map(|e| e == "txt").unwrap_or(false) {
            if let Some(filename) = path.file_name() {
                let output_file = output_path.join(filename);
                
                match fs::read_to_string(&path) {
                    Ok(content) => {
                        let (text, lines_joined, words_dehyphenated, spaces_normalized) = unwrap_lines_internal(&content);
                        
                        if let Err(e) = fs::write(&output_file, &text) {
                            eprintln!("Warning: Failed to write {}: {}", output_file.display(), e);
                            continue;
                        }
                        
                        total_files += 1;
                        total_lines_joined += lines_joined;
                        total_words_dehyphenated += words_dehyphenated;
                        total_spaces_normalized += spaces_normalized;
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to read {}: {}", path.display(), e);
                    }
                }
            }
        }
    }
    
    Ok((total_files, total_lines_joined, total_words_dehyphenated, total_spaces_normalized))
}

#[pymodule]
fn rust_ocr_clean(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(clean_text, m)?)?;
    m.add_function(wrap_pyfunction!(clean_text_with_categories, m)?)?;
    m.add_function(wrap_pyfunction!(clean_file_to_file, m)?)?;
    m.add_function(wrap_pyfunction!(clean_batch_parallel, m)?)?;
    m.add_class::<CleanupResult>()?;
    m.add_class::<BatchStats>()?;
    m.add_function(wrap_pyfunction!(extract_vocab_from_file, m)?)?;
    m.add_function(wrap_pyfunction!(extract_vocab_batch, m)?)?;
    m.add_function(wrap_pyfunction!(count_context_patterns, m)?)?;
    m.add_function(wrap_pyfunction!(count_context_patterns_file, m)?)?;
    m.add_function(wrap_pyfunction!(count_context_patterns_batch, m)?)?;
    m.add_function(wrap_pyfunction!(triage_text, m)?)?;
    m.add_function(wrap_pyfunction!(triage_file, m)?)?;
    m.add_function(wrap_pyfunction!(triage_batch, m)?)?;
    // New preprocessing functions
    m.add_function(wrap_pyfunction!(detect_language, m)?)?;
    m.add_function(wrap_pyfunction!(detect_language_file, m)?)?;
    m.add_function(wrap_pyfunction!(fix_unicode, m)?)?;
    m.add_function(wrap_pyfunction!(fix_unicode_file, m)?)?;
    m.add_function(wrap_pyfunction!(preprocess_text, m)?)?;
    m.add_function(wrap_pyfunction!(preprocess_file, m)?)?;
    // Dictionary functions
    m.add_function(wrap_pyfunction!(pattern_count, m)?)?;
    m.add_function(wrap_pyfunction!(init_dictionaries, m)?)?;
    m.add_function(wrap_pyfunction!(init_whitelist, m)?)?;
    m.add_function(wrap_pyfunction!(is_known_word, m)?)?;
    m.add_function(wrap_pyfunction!(word_languages, m)?)?;
    m.add_function(wrap_pyfunction!(dictionaries_loaded, m)?)?;
    m.add_class::<WordInfo>()?;
    m.add_class::<TriageResult>()?;
    m.add_class::<LangDetectResult>()?;
    m.add_class::<PreprocessResult>()?;
    // Line unwrapping functions
    m.add_function(wrap_pyfunction!(unwrap_lines, m)?)?;
    m.add_function(wrap_pyfunction!(unwrap_lines_file, m)?)?;
    m.add_function(wrap_pyfunction!(unwrap_lines_batch, m)?)?;
    m.add_class::<UnwrapResult>()?;
    // Boilerplate stripping functions
    m.add_function(wrap_pyfunction!(strip_boilerplate, m)?)?;
    m.add_function(wrap_pyfunction!(strip_boilerplate_file, m)?)?;
    m.add_function(wrap_pyfunction!(strip_boilerplate_batch, m)?)?;
    m.add_class::<BoilerplateResult>()?;
    m.add_class::<StrippedRegion>()?;
    Ok(())
}
