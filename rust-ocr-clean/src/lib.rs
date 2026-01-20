use pyo3::prelude::*;
use regex::Regex;
use lazy_static::lazy_static;

// Pre-compile all OCR patterns at module load time
lazy_static! {
    static ref OCR_PATTERNS: Vec<(Regex, &'static str, Option<Regex>)> = {
        vec![
            // 'the' variants (most common)
            (Regex::new(r"(?i)\btbe\b").unwrap(), "the", None),
            (Regex::new(r"(?i)\btlie\b").unwrap(), "the", None),
            (Regex::new(r"(?i)\btiie\b").unwrap(), "the", None),
            (Regex::new(r"(?i)\btbc\b").unwrap(), "the", None),
            (Regex::new(r"(?i)\bihe\b").unwrap(), "the", None),
            (Regex::new(r"(?i)\btne\b").unwrap(), "the", None),
            (Regex::new(r"(?i)\bthc\b").unwrap(), "the", None),
            (Regex::new(r"(?i)\bllie\b").unwrap(), "the", None),
            (Regex::new(r"(?i)\bllic\b").unwrap(), "the", None),
            (Regex::new(r"(?i)\bllio\b").unwrap(), "the", None),
            
            // 'this' variants
            (Regex::new(r"(?i)\btbis\b").unwrap(), "this", None),
            (Regex::new(r"(?i)\bthia\b").unwrap(), "this", None),
            (Regex::new(r"(?i)\btliis\b").unwrap(), "this", None),
            
            // 'that' variants
            (Regex::new(r"(?i)\btbat\b").unwrap(), "that", None),
            (Regex::new(r"(?i)\btliat\b").unwrap(), "that", None),
            (Regex::new(r"(?i)\btlmt\b").unwrap(), "that", None),
            (Regex::new(r"(?i)\bthnt\b").unwrap(), "that", None),
            
            // 'which' variants
            (Regex::new(r"(?i)\bwbich\b").unwrap(), "which", None),
            (Regex::new(r"(?i)\bwhicb\b").unwrap(), "which", None),
            (Regex::new(r"(?i)\bwliich\b").unwrap(), "which", None),
            (Regex::new(r"(?i)\bwliicli\b").unwrap(), "which", None),
            
            // 'what' variants
            (Regex::new(r"(?i)\bwliat\b").unwrap(), "what", None),
            (Regex::new(r"(?i)\bwlmt\b").unwrap(), "what", None),
            
            // 'when' variants
            (Regex::new(r"(?i)\bwlien\b").unwrap(), "when", None),
            (Regex::new(r"(?i)\bwben\b").unwrap(), "when", None),
            
            // 'where' variants
            (Regex::new(r"(?i)\bwliere\b").unwrap(), "where", None),
            (Regex::new(r"(?i)\bwbere\b").unwrap(), "where", None),
            
            // 'while' variants
            (Regex::new(r"(?i)\bwliile\b").unwrap(), "while", None),
            (Regex::new(r"(?i)\bwbile\b").unwrap(), "while", None),
            
            // 'who' variants
            (Regex::new(r"(?i)\bwlio\b").unwrap(), "who", None),
            (Regex::new(r"(?i)\bwliose\b").unwrap(), "whose", None),
            
            // 'him' variants
            (Regex::new(r"(?i)\bliim\b").unwrap(), "him", None),
            (Regex::new(r"(?i)\bhirn\b").unwrap(), "him", None),
            
            // 'his' variants
            (Regex::new(r"(?i)\bliis\b").unwrap(), "his", None),
            (Regex::new(r"(?i)\bhia\b").unwrap(), "his", None),
            
            // 'her' variants
            (Regex::new(r"(?i)\blier\b").unwrap(), "her", None),
            
            // 'he' - needs context
            (
                Regex::new(r"(?i)\blie\b").unwrap(),
                "he",
                Some(Regex::new(r"(?i)\b(and|but|that|when|if|as|so|because)\s+lie\b").unwrap())
            ),
            
            // 'she' variants
            (Regex::new(r"(?i)\bslie\b").unwrap(), "she", None),
            
            // 'they' variants
            (Regex::new(r"(?i)\btliey\b").unwrap(), "they", None),
            (Regex::new(r"(?i)\btbey\b").unwrap(), "they", None),
            
            // 'their' variants
            (Regex::new(r"(?i)\btbeir\b").unwrap(), "their", None),
            (Regex::new(r"(?i)\btlieir\b").unwrap(), "their", None),
            
            // 'them' variants
            (Regex::new(r"(?i)\btbem\b").unwrap(), "them", None),
            (Regex::new(r"(?i)\btliem\b").unwrap(), "them", None),
            
            // 'then' variants
            (Regex::new(r"(?i)\btben\b").unwrap(), "then", None),
            (Regex::new(r"(?i)\btlien\b").unwrap(), "then", None),
            
            // 'there' variants
            (Regex::new(r"(?i)\btbere\b").unwrap(), "there", None),
            (Regex::new(r"(?i)\btliere\b").unwrap(), "there", None),
            
            // 'these' variants
            (Regex::new(r"(?i)\btbese\b").unwrap(), "these", None),
            (Regex::new(r"(?i)\btliese\b").unwrap(), "these", None),
            
            // 'those' variants
            (Regex::new(r"(?i)\btbose\b").unwrap(), "those", None),
            (Regex::new(r"(?i)\btliose\b").unwrap(), "those", None),
            
            // 'other' variants
            (Regex::new(r"(?i)\botber\b").unwrap(), "other", None),
            (Regex::new(r"(?i)\botlier\b").unwrap(), "other", None),
            
            // 'and' variants
            (Regex::new(r"(?i)\barid\b").unwrap(), "and", None),
            (Regex::new(r"(?i)\baud\b").unwrap(), "and", None),
            (Regex::new(r"(?i)\bnnd\b").unwrap(), "and", None),
            (Regex::new(r"(?i)\baiid\b").unwrap(), "and", None),
            
            // 'with' variants
            (Regex::new(r"(?i)\bwitb\b").unwrap(), "with", None),
            (Regex::new(r"(?i)\bwitli\b").unwrap(), "with", None),
            
            // 'have' variants
            (Regex::new(r"(?i)\bhavo\b").unwrap(), "have", None),
            (Regex::new(r"(?i)\bbave\b").unwrap(), "have", None),
            (Regex::new(r"(?i)\bliave\b").unwrap(), "have", None),
            
            // Other common variants
            (Regex::new(r"(?i)\bboen\b").unwrap(), "been", None),
            (Regex::new(r"(?i)\bfrorn\b").unwrap(), "from", None),
            (Regex::new(r"(?i)\bwero\b").unwrap(), "were", None),
            (Regex::new(r"(?i)\bwonld\b").unwrap(), "would", None),
            (Regex::new(r"(?i)\bwouid\b").unwrap(), "would", None),
            (Regex::new(r"(?i)\bconld\b").unwrap(), "could", None),
            (Regex::new(r"(?i)\bcouid\b").unwrap(), "could", None),
            (Regex::new(r"(?i)\bsbould\b").unwrap(), "should", None),
            (Regex::new(r"(?i)\bshouid\b").unwrap(), "should", None),
            (Regex::new(r"(?i)\bbeiug\b").unwrap(), "being", None),
            (Regex::new(r"(?i)\bmado\b").unwrap(), "made", None),
            (Regex::new(r"(?i)\bnpon\b").unwrap(), "upon", None),
            (Regex::new(r"(?i)\bsucb\b").unwrap(), "such", None),
            (Regex::new(r"(?i)\bsucli\b").unwrap(), "such", None),
            (Regex::new(r"(?i)\bsomo\b").unwrap(), "some", None),
            (Regex::new(r"(?i)\bverv\b").unwrap(), "very", None),
            (Regex::new(r"(?i)\bllrst\b").unwrap(), "first", None),
            (Regex::new(r"(?i)\bfirst\b").unwrap(), "first", None),
            (Regex::new(r"(?i)\bftill\b").unwrap(), "still", None),
            
            // Long s (ſ -> s) - VERY common in old texts
            (Regex::new(r"ſ").unwrap(), "s", None),
            
            // Long-s OCR artifacts (ſ misread as f)
            (Regex::new(r"(?i)\bfuch\b").unwrap(), "such", None),
            (Regex::new(r"(?i)\bfome\b").unwrap(), "some", None),
            (Regex::new(r"(?i)\bfame\b").unwrap(), "same", None),
            (Regex::new(r"(?i)\bfaid\b").unwrap(), "said", None),
            (Regex::new(r"(?i)\bfays\b").unwrap(), "says", None),
            (Regex::new(r"(?i)\bfay\b").unwrap(), "say", None),
            (Regex::new(r"(?i)\bfaw\b").unwrap(), "saw", None),
            (Regex::new(r"(?i)\bfee\b").unwrap(), "see", None),
            (Regex::new(r"(?i)\bfeen\b").unwrap(), "seen", None),
            (Regex::new(r"(?i)\bfeems\b").unwrap(), "seems", None),
            (Regex::new(r"(?i)\bfeem\b").unwrap(), "seem", None),
            (Regex::new(r"(?i)\bfelf\b").unwrap(), "self", None),
            (Regex::new(r"(?i)\bfent\b").unwrap(), "sent", None),
            (Regex::new(r"(?i)\bfet\b").unwrap(), "set", None),
            (Regex::new(r"(?i)\bfhall\b").unwrap(), "shall", None),
            (Regex::new(r"(?i)\bfhould\b").unwrap(), "should", None),
            (Regex::new(r"(?i)\bfhe\b").unwrap(), "she", None),
            (Regex::new(r"(?i)\bfide\b").unwrap(), "side", None),
            (Regex::new(r"(?i)\bfince\b").unwrap(), "since", None),
            (Regex::new(r"(?i)\bfir\b").unwrap(), "sir", None),
            (Regex::new(r"(?i)\bfmall\b").unwrap(), "small", None),
            (Regex::new(r"(?i)\bfo\b").unwrap(), "so", None),
            (Regex::new(r"(?i)\bfon\b").unwrap(), "son", None),
            (Regex::new(r"(?i)\bfoon\b").unwrap(), "soon", None),
            (Regex::new(r"(?i)\bfoul\b").unwrap(), "soul", None),
            (Regex::new(r"(?i)\bfpeak\b").unwrap(), "speak", None),
            (Regex::new(r"(?i)\bfpoke\b").unwrap(), "spoke", None),
            (Regex::new(r"(?i)\bftand\b").unwrap(), "stand", None),
            (Regex::new(r"(?i)\bftate\b").unwrap(), "state", None),
            (Regex::new(r"(?i)\bftates\b").unwrap(), "states", None),
            (Regex::new(r"(?i)\bftill\b").unwrap(), "still", None),
            (Regex::new(r"(?i)\bftood\b").unwrap(), "stood", None),
            (Regex::new(r"(?i)\bftrong\b").unwrap(), "strong", None),
            (Regex::new(r"(?i)\bfubject\b").unwrap(), "subject", None),
            (Regex::new(r"(?i)\bfuffer\b").unwrap(), "suffer", None),
            (Regex::new(r"(?i)\bfupport\b").unwrap(), "support", None),
            (Regex::new(r"(?i)\bfure\b").unwrap(), "sure", None),
            (Regex::new(r"(?i)\bfyftem\b").unwrap(), "system", None),
            
            // rn/m confusion
            (Regex::new(r"(?i)\brnay\b").unwrap(), "may", None),
            (Regex::new(r"(?i)\brnuch\b").unwrap(), "much", None),
            (Regex::new(r"(?i)\brnore\b").unwrap(), "more", None),
            (Regex::new(r"(?i)\bsarne\b").unwrap(), "same", None),
            (Regex::new(r"(?i)\btirne\b").unwrap(), "time", None),
            (Regex::new(r"(?i)\bnarne\b").unwrap(), "name", None),
            (Regex::new(r"(?i)\bcorne\b").unwrap(), "come", None),
            (Regex::new(r"(?i)\bhorne\b").unwrap(), "home", None),
            (Regex::new(r"(?i)\bconntry\b").unwrap(), "country", None),
            (Regex::new(r"(?i)\bhnndred\b").unwrap(), "hundred", None),
            
            // ll -> U confusion (VERY common in this corpus)
            (Regex::new(r"(?i)\bwiU\b").unwrap(), "will", None),
            (Regex::new(r"(?i)\bweU\b").unwrap(), "well", None),
            (Regex::new(r"(?i)\bfuU\b").unwrap(), "full", None),
            (Regex::new(r"(?i)\bsmaU\b").unwrap(), "small", None),
            (Regex::new(r"(?i)\bstiU\b").unwrap(), "still", None),
            (Regex::new(r"(?i)\bcaUed\b").unwrap(), "called", None),
            (Regex::new(r"(?i)\bcaUing\b").unwrap(), "calling", None),
            (Regex::new(r"(?i)\bfoUow\b").unwrap(), "follow", None),
            (Regex::new(r"(?i)\bfoUows\b").unwrap(), "follows", None),
            (Regex::new(r"(?i)\bfoUowing\b").unwrap(), "following", None),
            (Regex::new(r"(?i)\bfoUowed\b").unwrap(), "followed", None),
            (Regex::new(r"(?i)\bshaU\b").unwrap(), "shall", None),
            (Regex::new(r"(?i)\bfeU\b").unwrap(), "fell", None),
            (Regex::new(r"(?i)\bteU\b").unwrap(), "tell", None),
            (Regex::new(r"(?i)\bseU\b").unwrap(), "sell", None),
            (Regex::new(r"(?i)\bfiU\b").unwrap(), "fill", None),
            (Regex::new(r"(?i)\bkiU\b").unwrap(), "kill", None),
            (Regex::new(r"(?i)\bskiU\b").unwrap(), "skill", None),
            (Regex::new(r"(?i)\bmiU\b").unwrap(), "mill", None),
            (Regex::new(r"(?i)\bbiU\b").unwrap(), "bill", None),
            (Regex::new(r"(?i)\bhiU\b").unwrap(), "hill", None),
            (Regex::new(r"(?i)\btiU\b").unwrap(), "till", None),
            (Regex::new(r"(?i)\bpuU\b").unwrap(), "pull", None),
            (Regex::new(r"(?i)\baU\b").unwrap(), "all", None),
            (Regex::new(r"(?i)\bbaU\b").unwrap(), "ball", None),
            (Regex::new(r"(?i)\bwaU\b").unwrap(), "wall", None),
            (Regex::new(r"(?i)\bfaU\b").unwrap(), "fall", None),
            (Regex::new(r"(?i)\bcaU\b").unwrap(), "call", None),
            (Regex::new(r"(?i)\btaU\b").unwrap(), "tall", None),
            (Regex::new(r"(?i)\bdoUars\b").unwrap(), "dollars", None),
            (Regex::new(r"(?i)\bcoUege\b").unwrap(), "college", None),
            (Regex::new(r"(?i)\bcoUection\b").unwrap(), "collection", None),
            (Regex::new(r"(?i)\bexceUent\b").unwrap(), "excellent", None),
            (Regex::new(r"(?i)\binteUigent\b").unwrap(), "intelligent", None),
            (Regex::new(r"(?i)\binteUigence\b").unwrap(), "intelligence", None),
            // More ll -> U common words
            (Regex::new(r"(?i)\bpubUc\b").unwrap(), "public", None),
            (Regex::new(r"(?i)\bengUsh\b").unwrap(), "English", None),
            (Regex::new(r"(?i)\bheaUh\b").unwrap(), "health", None),
            (Regex::new(r"(?i)\blitUe\b").unwrap(), "little", None),
            (Regex::new(r"(?i)\bfuUy\b").unwrap(), "fully", None),
            (Regex::new(r"(?i)\bfeUow\b").unwrap(), "fellow", None),
            (Regex::new(r"(?i)\bparUament\b").unwrap(), "parliament", None),
            (Regex::new(r"(?i)\bmiUtary\b").unwrap(), "military", None),
            (Regex::new(r"(?i)\bmUe\b").unwrap(), "mile", None),
            (Regex::new(r"(?i)\bmUes\b").unwrap(), "miles", None),
            (Regex::new(r"(?i)\bpoUcy\b").unwrap(), "policy", None),
            (Regex::new(r"(?i)\bappUed\b").unwrap(), "applied", None),
            (Regex::new(r"(?i)\bappUy\b").unwrap(), "apply", None),
            (Regex::new(r"(?i)\bappUcation\b").unwrap(), "application", None),
            (Regex::new(r"(?i)\bappUcations\b").unwrap(), "applications", None),
            (Regex::new(r"(?i)\bestabUshed\b").unwrap(), "established", None),
            (Regex::new(r"(?i)\bgenUeman\b").unwrap(), "gentleman", None),
            (Regex::new(r"(?i)\bgeneraUy\b").unwrap(), "generally", None),
            (Regex::new(r"(?i)\baUowed\b").unwrap(), "allowed", None),
            (Regex::new(r"(?i)\baUow\b").unwrap(), "allow", None),
            (Regex::new(r"(?i)\bviUage\b").unwrap(), "village", None),
            (Regex::new(r"(?i)\bviUages\b").unwrap(), "villages", None),
            (Regex::new(r"(?i)\bwhoUy\b").unwrap(), "wholly", None),
            (Regex::new(r"(?i)\bbuUt\b").unwrap(), "built", None),
            (Regex::new(r"(?i)\byeUow\b").unwrap(), "yellow", None),
            (Regex::new(r"(?i)\bcoUonel\b").unwrap(), "colonel", None),
            (Regex::new(r"(?i)\bbeUeved\b").unwrap(), "believed", None),
            (Regex::new(r"(?i)\bbeUeve\b").unwrap(), "believe", None),
            (Regex::new(r"(?i)\bbeUef\b").unwrap(), "belief", None),
            (Regex::new(r"(?i)\bmiUions\b").unwrap(), "millions", None),
            (Regex::new(r"(?i)\bmiUion\b").unwrap(), "million", None),
            (Regex::new(r"(?i)\bdaUy\b").unwrap(), "daily", None),
            (Regex::new(r"(?i)\bdeUvered\b").unwrap(), "delivered", None),
            (Regex::new(r"(?i)\bdeUver\b").unwrap(), "deliver", None),
            (Regex::new(r"(?i)\bvaUey\b").unwrap(), "valley", None),
            (Regex::new(r"(?i)\bvaUeys\b").unwrap(), "valleys", None),
            (Regex::new(r"(?i)\bkiUed\b").unwrap(), "killed", None),
            (Regex::new(r"(?i)\bespeciaUy\b").unwrap(), "especially", None),
            (Regex::new(r"(?i)\bchUdren\b").unwrap(), "children", None),
            (Regex::new(r"(?i)\bfeeUng\b").unwrap(), "feeling", None),
            (Regex::new(r"(?i)\bfeeUngs\b").unwrap(), "feelings", None),
            (Regex::new(r"(?i)\bfamUy\b").unwrap(), "family", None),
            (Regex::new(r"(?i)\bfamUies\b").unwrap(), "families", None),
            (Regex::new(r"(?i)\bhoUow\b").unwrap(), "hollow", None),
            (Regex::new(r"(?i)\bfaUen\b").unwrap(), "fallen", None),
            (Regex::new(r"(?i)\bfaUing\b").unwrap(), "falling", None),
            (Regex::new(r"(?i)\bpoUtics\b").unwrap(), "politics", None),
            (Regex::new(r"(?i)\bpoUtical\b").unwrap(), "political", None),
            (Regex::new(r"(?i)\brebeUion\b").unwrap(), "rebellion", None),
            (Regex::new(r"(?i)\baUies\b").unwrap(), "allies", None),
            (Regex::new(r"(?i)\baUied\b").unwrap(), "allied", None),
            (Regex::new(r"(?i)\bequaUy\b").unwrap(), "equally", None),
            (Regex::new(r"(?i)\busuaUy\b").unwrap(), "usually", None),
            (Regex::new(r"(?i)\bquaUty\b").unwrap(), "quality", None),
            (Regex::new(r"(?i)\bcoUected\b").unwrap(), "collected", None),
            (Regex::new(r"(?i)\bcoUect\b").unwrap(), "collect", None),
            (Regex::new(r"(?i)\braUroad\b").unwrap(), "railroad", None),
            (Regex::new(r"(?i)\boriginaUy\b").unwrap(), "originally", None),
            (Regex::new(r"(?i)\bbrUiant\b").unwrap(), "brilliant", None),
            (Regex::new(r"(?i)\brepubUc\b").unwrap(), "republic", None),
            (Regex::new(r"(?i)\bcathoUc\b").unwrap(), "catholic", None),
            (Regex::new(r"(?i)\bchanceUor\b").unwrap(), "chancellor", None),
            (Regex::new(r"(?i)\bprobaUy\b").unwrap(), "probably", None),
            (Regex::new(r"(?i)\bbuUding\b").unwrap(), "building", None),
            (Regex::new(r"(?i)\bbuUdings\b").unwrap(), "buildings", None),
            (Regex::new(r"(?i)\bentiUed\b").unwrap(), "entitled", None),
            (Regex::new(r"(?i)\bwooUen\b").unwrap(), "woollen", None),
            (Regex::new(r"(?i)\bmetropoUtan\b").unwrap(), "metropolitan", None),
            (Regex::new(r"(?i)\bitaUan\b").unwrap(), "Italian", None),
            (Regex::new(r"(?i)\biUustrated\b").unwrap(), "illustrated", None),
            (Regex::new(r"(?i)\biUustration\b").unwrap(), "illustration", None),
            (Regex::new(r"(?i)\bveUum\b").unwrap(), "vellum", None),
            (Regex::new(r"(?i)\bfoUo\b").unwrap(), "folio", None),
            // Proper names with ll -> U
            (Regex::new(r"\bWiUiam\b").unwrap(), "William", None),
            (Regex::new(r"\bWilUam\b").unwrap(), "William", None),
            (Regex::new(r"\bWiUiams\b").unwrap(), "Williams", None),
            (Regex::new(r"\bPhiUip\b").unwrap(), "Philip", None),
            (Regex::new(r"\bPhiUppine\b").unwrap(), "Philippine", None),
            (Regex::new(r"\bPhiUppines\b").unwrap(), "Philippines", None),
            (Regex::new(r"\bDubUn\b").unwrap(), "Dublin", None),
            (Regex::new(r"\bBerUn\b").unwrap(), "Berlin", None),
            (Regex::new(r"\bApoUo\b").unwrap(), "Apollo", None),
            (Regex::new(r"\blUinois\b").unwrap(), "Illinois", None),
            (Regex::new(r"\bCaroUna\b").unwrap(), "Carolina", None),
            (Regex::new(r"\bNashviUe\b").unwrap(), "Nashville", None),
            (Regex::new(r"\bHoUand\b").unwrap(), "Holland", None),
            (Regex::new(r"\bViUa\b").unwrap(), "Villa", None),
            
            // Additional h/li errors from corpus analysis
            (Regex::new(r"(?i)\btke\b").unwrap(), "the", None),
            (Regex::new(r"(?i)\bsnch\b").unwrap(), "such", None),
            (Regex::new(r"(?i)\bmnch\b").unwrap(), "much", None),
            (Regex::new(r"(?i)\bmnst\b").unwrap(), "must", None),
            (Regex::new(r"(?i)\bthns\b").unwrap(), "thus", None),
            (Regex::new(r"(?i)\bwonld\b").unwrap(), "would", None),
            (Regex::new(r"(?i)\bconld\b").unwrap(), "could", None),
            (Regex::new(r"(?i)\bsliould\b").unwrap(), "should", None),
            (Regex::new(r"(?i)\bliave\b").unwrap(), "have", None),
            (Regex::new(r"(?i)\bliaving\b").unwrap(), "having", None),
            (Regex::new(r"(?i)\bliead\b").unwrap(), "head", None),
            (Regex::new(r"(?i)\blieart\b").unwrap(), "heart", None),
            (Regex::new(r"(?i)\bliand\b").unwrap(), "hand", None),
            (Regex::new(r"(?i)\bliouse\b").unwrap(), "house", None),
            (Regex::new(r"(?i)\bliow\b").unwrap(), "how", None),
            (Regex::new(r"(?i)\bliope\b").unwrap(), "hope", None),
            (Regex::new(r"(?i)\bliere\b").unwrap(), "here", None),
            (Regex::new(r"(?i)\bliigh\b").unwrap(), "high", None),
            (Regex::new(r"(?i)\bliistory\b").unwrap(), "history", None),
            (Regex::new(r"(?i)\blialf\b").unwrap(), "half", None),
            (Regex::new(r"(?i)\bliold\b").unwrap(), "hold", None),
            (Regex::new(r"(?i)\blioly\b").unwrap(), "holy", None),
            (Regex::new(r"(?i)\blionor\b").unwrap(), "honor", None),
            (Regex::new(r"(?i)\blionour\b").unwrap(), "honour", None),
            // More li/h errors (tli->th, lli->ll patterns)
            (Regex::new(r"(?i)\btliis\b").unwrap(), "this", None),
            (Regex::new(r"(?i)\btliia\b").unwrap(), "this", None),
            (Regex::new(r"(?i)\btliat\b").unwrap(), "that", None),
            (Regex::new(r"(?i)\btlie\b").unwrap(), "the", None),
            (Regex::new(r"(?i)\btlien\b").unwrap(), "then", None),
            (Regex::new(r"(?i)\btliere\b").unwrap(), "there", None),
            (Regex::new(r"(?i)\btliey\b").unwrap(), "they", None),
            (Regex::new(r"(?i)\btliem\b").unwrap(), "them", None),
            (Regex::new(r"(?i)\btlieir\b").unwrap(), "their", None),
            (Regex::new(r"(?i)\btliese\b").unwrap(), "these", None),
            (Regex::new(r"(?i)\btliose\b").unwrap(), "those", None),
            (Regex::new(r"(?i)\btliough\b").unwrap(), "though", None),
            (Regex::new(r"(?i)\btlirough\b").unwrap(), "through", None),
            (Regex::new(r"(?i)\btliink\b").unwrap(), "think", None),
            (Regex::new(r"(?i)\btliings\b").unwrap(), "things", None),
            (Regex::new(r"(?i)\btliing\b").unwrap(), "thing", None),
            (Regex::new(r"(?i)\bwliich\b").unwrap(), "which", None),
            (Regex::new(r"(?i)\bwliile\b").unwrap(), "while", None),
            (Regex::new(r"(?i)\bwlien\b").unwrap(), "when", None),
            (Regex::new(r"(?i)\bwliat\b").unwrap(), "what", None),
            (Regex::new(r"(?i)\bwliere\b").unwrap(), "where", None),
            (Regex::new(r"(?i)\bwliether\b").unwrap(), "whether", None),
            (Regex::new(r"(?i)\bwliole\b").unwrap(), "whole", None),
            (Regex::new(r"(?i)\bwliom\b").unwrap(), "whom", None),
            (Regex::new(r"(?i)\bwliose\b").unwrap(), "whose", None),
            (Regex::new(r"(?i)\bcliild\b").unwrap(), "child", None),
            (Regex::new(r"(?i)\bcliildren\b").unwrap(), "children", None),
            (Regex::new(r"(?i)\bcliief\b").unwrap(), "chief", None),
            (Regex::new(r"(?i)\bcliurch\b").unwrap(), "church", None),
            (Regex::new(r"(?i)\bnotliing\b").unwrap(), "nothing", None),
            (Regex::new(r"(?i)\bsometliing\b").unwrap(), "something", None),
            (Regex::new(r"(?i)\beverytliing\b").unwrap(), "everything", None),
            (Regex::new(r"(?i)\banytliing\b").unwrap(), "anything", None),
            (Regex::new(r"(?i)\bliigli\b").unwrap(), "high", None),
            (Regex::new(r"(?i)\bliigh\b").unwrap(), "high", None),
            (Regex::new(r"(?i)\bliiglier\b").unwrap(), "higher", None),
            (Regex::new(r"(?i)\bliigliest\b").unwrap(), "highest", None),
            (Regex::new(r"(?i)\blliey\b").unwrap(), "they", None),
            (Regex::new(r"(?i)\blliere\b").unwrap(), "there", None),
            (Regex::new(r"(?i)\blliat\b").unwrap(), "that", None),
            
            // More long-s artifacts
            (Regex::new(r"(?i)\bhimfelf\b").unwrap(), "himself", None),
            (Regex::new(r"(?i)\bherfelf\b").unwrap(), "herself", None),
            (Regex::new(r"(?i)\bitfelf\b").unwrap(), "itself", None),
            (Regex::new(r"(?i)\bmyfelf\b").unwrap(), "myself", None),
            (Regex::new(r"(?i)\byourfelf\b").unwrap(), "yourself", None),
            (Regex::new(r"(?i)\bthemfelves\b").unwrap(), "themselves", None),
            (Regex::new(r"(?i)\bourfelves\b").unwrap(), "ourselves", None),
            (Regex::new(r"(?i)\bfufficient\b").unwrap(), "sufficient", None),
            (Regex::new(r"(?i)\bfuflScient\b").unwrap(), "sufficient", None),
            (Regex::new(r"(?i)\bfuccefsful\b").unwrap(), "successful", None),
            (Regex::new(r"(?i)\bfuccefs\b").unwrap(), "success", None),
            (Regex::new(r"(?i)\bnecefsary\b").unwrap(), "necessary", None),
            (Regex::new(r"(?i)\bpoffible\b").unwrap(), "possible", None),
            (Regex::new(r"(?i)\bimpoffible\b").unwrap(), "impossible", None),
            (Regex::new(r"(?i)\bpoffefs\b").unwrap(), "possess", None),
            (Regex::new(r"(?i)\bpoffeffion\b").unwrap(), "possession", None),
            (Regex::new(r"(?i)\bpaffage\b").unwrap(), "passage", None),
            (Regex::new(r"(?i)\bpaffed\b").unwrap(), "passed", None),
            (Regex::new(r"(?i)\bpafs\b").unwrap(), "pass", None),
            (Regex::new(r"(?i)\bclafses\b").unwrap(), "classes", None),
            (Regex::new(r"(?i)\bclafs\b").unwrap(), "class", None),
            (Regex::new(r"(?i)\bmafs\b").unwrap(), "mass", None),
            (Regex::new(r"(?i)\blefs\b").unwrap(), "less", None),
            (Regex::new(r"(?i)\bunlefs\b").unwrap(), "unless", None),
            (Regex::new(r"(?i)\bbufinefs\b").unwrap(), "business", None),
            (Regex::new(r"(?i)\bcongrefs\b").unwrap(), "congress", None),
            (Regex::new(r"(?i)\bprogrefs\b").unwrap(), "progress", None),
            (Regex::new(r"(?i)\bexprefs\b").unwrap(), "express", None),
            (Regex::new(r"(?i)\bpoffefs\b").unwrap(), "possess", None),
            (Regex::new(r"(?i)\bwouM\b").unwrap(), "would", None),
            (Regex::new(r"(?i)\bcouM\b").unwrap(), "could", None),
            (Regex::new(r"(?i)\bshouM\b").unwrap(), "should", None),
            
            // More ll -> U variants (WlU, Wili patterns)
            (Regex::new(r"(?i)\bwlU\b").unwrap(), "will", None),
            (Regex::new(r"(?i)\bwili\b").unwrap(), "will", None),
            (Regex::new(r"(?i)\bwiili\b").unwrap(), "will", None),
            (Regex::new(r"(?i)\bstlU\b").unwrap(), "still", None),
            (Regex::new(r"(?i)\bstili\b").unwrap(), "still", None),
            (Regex::new(r"(?i)\bfuily\b").unwrap(), "fully", None),
            (Regex::new(r"(?i)\bfiily\b").unwrap(), "fully", None),
            (Regex::new(r"(?i)\breaily\b").unwrap(), "really", None),
            (Regex::new(r"(?i)\bfinaily\b").unwrap(), "finally", None),
            (Regex::new(r"(?i)\bspeciaily\b").unwrap(), "specially", None),
            (Regex::new(r"(?i)\bactuaily\b").unwrap(), "actually", None),
            (Regex::new(r"(?i)\bnaturaily\b").unwrap(), "naturally", None),
            
            // ii/n confusion
            (Regex::new(r"(?i)\bkiiow\b").unwrap(), "know", None),
            (Regex::new(r"(?i)\bkiiown\b").unwrap(), "known", None),
            (Regex::new(r"(?i)\btiiis\b").unwrap(), "this", None),
            (Regex::new(r"(?i)\bwiiich\b").unwrap(), "which", None),
            (Regex::new(r"(?i)\bcliildren\b").unwrap(), "children", None),
            
            // cl/d confusion
            (
                Regex::new(r"(?i)\bclo\b").unwrap(),
                "do",
                Some(Regex::new(r"(?i)\b(to|not|can|will|shall|would|could)\s+clo\b").unwrap())
            ),
            
            // Ligatures
            (Regex::new(r"ﬁ").unwrap(), "fi", None),
            (Regex::new(r"ﬂ").unwrap(), "fl", None),
            (Regex::new(r"ﬀ").unwrap(), "ff", None),
            (Regex::new(r"ﬃ").unwrap(), "ffi", None),
            (Regex::new(r"ﬄ").unwrap(), "ffl", None),
            
            // Google watermarks and digitization artifacts
            (Regex::new(r"(?i)\bVjOOQIC\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bVjOOQLC\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bVjOOQ\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bLjOOQIC\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bLjOOQ\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bLiOOQLC\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bCjOOQIC\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bCjOOQlC\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bCjOOQ\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bbyVjOOQlC\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bbyVrrOOQlC\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bbyCjOOQlC\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bhyGoogIc\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bbyGoogk\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bbyGoogle\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bGoOglc\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bGoogXt\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bOOglC\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bDigiLizedbyGoOglc\b").unwrap(), "", None),
            (Regex::new(r"(?i)Digitized\s+by\s+[VLC]j?OOQ(?:IC|LC|lC)").unwrap(), "", None),
            (Regex::new(r"(?i)Digitized\s+by\s+Google").unwrap(), "", None),
            (Regex::new(r"(?i)\bdbyGoogle\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bbyGoogle\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bOOglC\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bLiOOQ\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bVjOCK\b").unwrap(), "", None),
            // Anachronisms (modern terms that shouldn't appear in pre-WWI text)
            (Regex::new(r"(?i)\bgoogle\b").unwrap(), "", None),
            (Regex::new(r"(?i)\binternet\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bwebsite\b").unwrap(), "", None),
            
            // ff ligature errors (oflSce pattern)
            (Regex::new(r"(?i)\boflSce\b").unwrap(), "office", None),
            (Regex::new(r"(?i)\boflScer\b").unwrap(), "officer", None),
            (Regex::new(r"(?i)\boflScers\b").unwrap(), "officers", None),
            (Regex::new(r"(?i)\boflScial\b").unwrap(), "official", None),
            (Regex::new(r"(?i)\bdifTerent\b").unwrap(), "different", None),
            (Regex::new(r"(?i)\bafTair\b").unwrap(), "affair", None),
            (Regex::new(r"(?i)\bafTairs\b").unwrap(), "affairs", None),
            (Regex::new(r"(?i)\bafTect\b").unwrap(), "affect", None),
            (Regex::new(r"(?i)\befTect\b").unwrap(), "effect", None),
            (Regex::new(r"(?i)\befTects\b").unwrap(), "effects", None),
            
            // Repeated letters (AAA, BBB, etc) - expanded since Rust regex doesn't support backreferences
            (Regex::new(r"(?i)\bAAA+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bBBB+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bDDD+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bEEE+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bFFF+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bGGG+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bHHH+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bJJJ+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bKKK+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bNNN+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bOOO+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bPPP+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bQQQ+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bRRR+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bSSS+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bTTT+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bUUU+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bWWW+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bYYY+\b").unwrap(), "", None),
            (Regex::new(r"(?i)\bZZZ+\b").unwrap(), "", None),
            
            // 2-letter noise
            (Regex::new(r"(?i)\b[I1]A\b").unwrap(), "", None),
            (Regex::new(r"(?i)\b[I1]H\b").unwrap(), "", None),
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

/// Clean OCR errors in text using pre-compiled patterns
#[pyfunction]
fn clean_text(text: String) -> PyResult<(String, u64)> {
    let (result, subs) = clean_text_internal(&text);
    Ok((result, subs))
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
/// Returns: (was_modified, substitution_count, bytes_read)
#[pyfunction]
fn clean_file_to_file(input_path: String, output_path: String) -> PyResult<(bool, u64, u64)> {
    use std::fs;
    use std::path::Path;

    // Read file
    let content = fs::read_to_string(&input_path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Failed to read {}: {}", input_path, e)))?;
    
    let bytes_read = content.len() as u64;
    
    // Clean content (reuse internal logic)
    let (cleaned, subs) = clean_text_internal(&content);
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

    Ok((was_modified, subs, bytes_read))
}

/// Internal clean function (not exposed to Python, avoids string copies)
fn clean_text_internal(text: &str) -> (String, u64) {
    let mut result = text.to_string();
    let mut total_subs: u64 = 0;

    for (pattern, replacement, context) in OCR_PATTERNS.iter() {
        if let Some(ctx_pattern) = context {
            // Contextual replacement - only replace if context matches
            result = ctx_pattern.replace_all(&result, |caps: &regex::Captures| {
                let matched = caps.get(0).unwrap().as_str();
                let replaced = pattern.replace_all(matched, *replacement);
                if replaced != matched {
                    total_subs += 1;
                }
                replaced.into_owned()
            }).into_owned();
        } else {
            // Direct replacement
            let before_count = pattern.find_iter(&result).count();
            if before_count > 0 {
                result = pattern.replace_all(&result, *replacement).into_owned();
                total_subs += before_count as u64;
            }
        }
    }

    (result, total_subs)
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
        (Regex::new(r"[il1|]{3,}").unwrap(), "confusable_chars"),    // l/1/|/i confusion
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

#[pymodule]
fn rust_ocr_clean(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(clean_text, m)?)?;
    m.add_function(wrap_pyfunction!(clean_file_to_file, m)?)?;
    m.add_function(wrap_pyfunction!(extract_vocab_from_file, m)?)?;
    m.add_function(wrap_pyfunction!(extract_vocab_batch, m)?)?;
    m.add_function(wrap_pyfunction!(count_context_patterns, m)?)?;
    m.add_function(wrap_pyfunction!(count_context_patterns_file, m)?)?;
    m.add_function(wrap_pyfunction!(count_context_patterns_batch, m)?)?;
    m.add_class::<WordInfo>()?;
    Ok(())
}
