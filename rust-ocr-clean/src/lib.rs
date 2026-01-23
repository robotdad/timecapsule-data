use pyo3::prelude::*;
use regex::Regex;
use lazy_static::lazy_static;
use unicode_normalization::UnicodeNormalization;
use whatlang::{detect, Lang};

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
            
            // ii/n confusion
            (Regex::new(r"(?i)\bkiiow\b").unwrap(), "know", None, "ii_n_confusion"),
            (Regex::new(r"(?i)\bkiiown\b").unwrap(), "known", None, "ii_n_confusion"),
            (Regex::new(r"(?i)\btiiis\b").unwrap(), "this", None, "ii_n_confusion"),
            (Regex::new(r"(?i)\bwiiich\b").unwrap(), "which", None, "ii_n_confusion"),
            (Regex::new(r"(?i)\bcliildren\b").unwrap(), "children", None, "ii_n_confusion"),
            
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
/// Returns: (was_modified, substitution_count, bytes_read, categories)
/// where categories is a HashMap of category_name -> count
#[pyfunction]
fn clean_file_to_file(input_path: String, output_path: String) -> PyResult<(bool, u64, u64, std::collections::HashMap<String, u64>)> {
    use std::fs;
    use std::path::Path;

    // Read file
    let content = fs::read_to_string(&input_path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Failed to read {}: {}", input_path, e)))?;
    
    let bytes_read = content.len() as u64;
    
    // Clean content (reuse internal logic)
    let (cleaned, subs, categories) = clean_text_internal(&content);
    let was_modified = subs > 0;

    // Ensure parent directory exists
    let out_path = Path::new(&output_path);
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Failed to create directory: {}", e)))?;
    }

    // Write output
    fs::write(out_path, &cleaned)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Failed to write {}: {}", output_path, e)))?;

    Ok((was_modified, subs, bytes_read, categories))
}

/// Internal clean function (not exposed to Python, avoids string copies)
/// Returns: (cleaned_text, total_substitutions, substitutions_by_category)
fn clean_text_internal(text: &str) -> (String, u64, std::collections::HashMap<String, u64>) {
    use std::collections::HashMap;
    
    let mut result = text.to_string();
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

lazy_static! {
    // Word extraction pattern
    static ref WORD_PATTERN: Regex = Regex::new(r"\b([a-zA-Z][a-zA-Z']*[a-zA-Z]|[a-zA-Z])\b").unwrap();
    
    // Suspicious patterns that suggest OCR errors
    // Note: Rust regex doesn't support backreferences, so we enumerate repeated chars
    static ref SUSPICIOUS_PATTERNS: Vec<(Regex, &'static str)> = vec![
        (Regex::new(r"[a-z][A-Z]").unwrap(), "camelCase"),           // camelCase in middle
        // Triple+ repeated chars (expanded since no backreferences)
        (Regex::new(r"(?i)(aaa|bbb|ccc|ddd|eee|fff|ggg|hhh|iii|jjj|kkk|lll|mmm|nnn|ooo|ppp|qqq|rrr|sss|ttt|uuu|vvv|www|xxx|yyy|zzz)").unwrap(), "triple_repeat"),
        (Regex::new(r"[^aeiouAEIOU]{5,}").unwrap(), "consonant_run"), // 5+ consonants
        (Regex::new(r"(?i)^[bcdfghjklmnpqrstvwxz]{4,}$").unwrap(), "all_consonants"), // All consonants 4+
        // Confusable char sequences - require actual OCR confusion markers (digits or pipe)
        // Old pattern r"[il1|]{3,}" caught legitimate words like "Still", "William", "Military"
        (Regex::new(r"[1|][il1|]+").unwrap(), "confusable_starts_digit"),  // Starts with digit/pipe (Wi1liam, fi|l)
        (Regex::new(r"[il1|]+[1|]").unwrap(), "confusable_ends_digit"),    // Ends with digit/pipe (Will1, fil|)
        (Regex::new(r"[rnm]{4,}").unwrap(), "rn_m_confusion"),       // rn/m confusion
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
        
        // Skip common words and very short words
        if word.len() < 2 || SKIP_WORDS.contains(word_lower.as_str()) {
            continue;
        }
        
        total_words += 1;
        
        // Get or create entry
        if !word_counts.contains_key(&word_lower) {
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
            
            if word.len() < 2 || SKIP_WORDS.contains(word_lower.as_str()) {
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

#[pymodule]
fn rust_ocr_clean(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(clean_text, m)?)?;
    m.add_function(wrap_pyfunction!(clean_text_with_categories, m)?)?;
    m.add_function(wrap_pyfunction!(clean_file_to_file, m)?)?;
    m.add_class::<CleanupResult>()?;
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
    m.add_function(wrap_pyfunction!(init_dictionaries, m)?)?;
    m.add_function(wrap_pyfunction!(is_known_word, m)?)?;
    m.add_function(wrap_pyfunction!(word_languages, m)?)?;
    m.add_function(wrap_pyfunction!(dictionaries_loaded, m)?)?;
    m.add_class::<WordInfo>()?;
    m.add_class::<TriageResult>()?;
    m.add_class::<LangDetectResult>()?;
    m.add_class::<PreprocessResult>()?;
    Ok(())
}
