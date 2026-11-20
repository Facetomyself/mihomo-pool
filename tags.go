package mihomopool

import (
	"regexp"
	"strconv"
	"strings"
)

// 节点标签提取。
//
// 真实机场的节点名信息密度很高，例如：
//
//	🇭🇰香港高速01|BGP|CMCU
//	🇯🇵日本专线03|BGP|流媒体
//	🇺🇸美国洛杉矶01|流媒体|0.01x
//	🇸🇬新加坡专线01|BGP|流媒体
//
// 这些信息对调度很有用：只想跑 0.1x 以下倍率的节点、只走流媒体解锁、
// 只要某地区的出口。把节点名解析成结构化标签后，筛选不用再写正则。

// Tag 是节点标签的类别前缀，便于按类别筛选。
const (
	// TagRegion 地区，值为国家/地区代码或中文名，如 region:HK。
	TagRegion = "region"
	// TagRate 倍率，如 rate:0.1 表示 0.1 倍计费。
	TagRate = "rate"
	// TagStream 支持流媒体解锁。
	TagStream = "stream"
	// TagDedicated 专线。
	TagDedicated = "dedicated"
	// TagBGP 标注 BGP 线路。
	TagBGP = "bgp"
	// TagHighSpeed 标注高速。
	TagHighSpeed = "speed"
	// TagExpiry 标注到期/剩余类信息（机场公告节点）。
	TagExpiry = "expiry"
)

// regionKeywords 按顺序匹配，长的放前面避免被短的抢先。
//
// 值统一用 ISO 3166-1 alpha-2，便于跨订阅比较（有的机场写「香港」，
// 有的写「HK」，有的写「Hong Kong」）。
var regionKeywords = []struct {
	code string
	keys []string
}{
	{"HK", []string{"香港", "hong kong", "hongkong", "🇭🇰", "hk"}},
	{"TW", []string{"台湾", "台灣", "taiwan", "🇹🇼", "tw"}},
	{"JP", []string{"日本", "东京", "大阪", "japan", "tokyo", "osaka", "🇯🇵", "jp"}},
	{"SG", []string{"新加坡", "狮城", "singapore", "🇸🇬", "sg"}},
	{"US", []string{"美国", "美國", "洛杉矶", "圣何塞", "硅谷", "西雅图", "纽约",
		"united states", "losangeles", "los angeles", "sanjose", "san jose",
		"seattle", "newyork", "new york", "🇺🇸", "us"}},
	{"GB", []string{"英国", "伦敦", "united kingdom", "london", "🇬🇧", "gb", "uk"}},
	{"KR", []string{"韩国", "首尔", "korea", "seoul", "🇰🇷", "kr"}},
	{"DE", []string{"德国", "法兰克福", "germany", "frankfurt", "🇩🇪", "de"}},
	{"FR", []string{"法国", "巴黎", "france", "paris", "🇫🇷", "fr"}},
	{"CA", []string{"加拿大", "多伦多", "canada", "toronto", "🇨🇦", "ca"}},
	{"AU", []string{"澳大利亚", "悉尼", "australia", "sydney", "🇦🇺", "au"}},
	{"IN", []string{"印度", "india", "🇮🇳", "in"}},
	{"RU", []string{"俄罗斯", "russia", "🇷🇺", "ru"}},
	{"TR", []string{"土耳其", "turkey", "🇹🇷", "tr"}},
	{"AR", []string{"阿根廷", "argentina", "🇦🇷", "ar"}},
	{"BR", []string{"巴西", "brazil", "🇧🇷", "br"}},
	{"NL", []string{"荷兰", "阿姆斯特丹", "netherlands", "amsterdam", "🇳🇱", "nl"}},
	{"CH", []string{"瑞士", "switzerland", "🇨🇭", "ch"}},
	{"SE", []string{"瑞典", "sweden", "🇸🇪", "se"}},
	{"MY", []string{"马来西亚", "malaysia", "🇲🇾", "my"}},
	{"TH", []string{"泰国", "thailand", "🇹🇭", "th"}},
	{"VN", []string{"越南", "vietnam", "🇻🇳", "vn"}},
	{"PH", []string{"菲律宾", "philippines", "🇵🇭", "ph"}},
	{"ID", []string{"印尼", "印度尼西亚", "indonesia", "🇮🇩", "id"}},
}

// rateRe 匹配倍率标记：0.1x、0.01x、1x、2.5x。
var rateRe = regexp.MustCompile(`(?i)(\d+(?:\.\d+)?)\s*x\b`)

// lowRateRe 匹配"低倍率"这种不带数字的描述。
var lowRateRe = regexp.MustCompile(`(?i)低倍率|高倍率`)

// ExtractTags 从节点名与类型中提取标签。
//
// 返回的标签形如 region:HK、rate:0.1、stream、dedicated、bgp、speed。
// 提取失败的类别直接不出现在结果里，不做兜底猜测。
func ExtractTags(name, nodeType string) []string {
	if strings.TrimSpace(name) == "" {
		return nil
	}
	lower := strings.ToLower(name)
	seen := make(map[string]bool, 6)
	out := make([]string, 0, 6)

	add := func(tag string) {
		if tag == "" || seen[tag] {
			return
		}
		seen[tag] = true
		out = append(out, tag)
	}

	// —— 地区 ——
	// 按关键词长度从长到短匹配，避免 "in" 命中 "singapore" 之类的误报。
	for _, r := range regionKeywords {
		if hasRegionKeyword(lower, name, r.keys) {
			add(TagRegion + ":" + r.code)
			break
		}
	}

	// —— 倍率 ——
	if m := rateRe.FindStringSubmatch(name); len(m) == 2 {
		if f, err := strconv.ParseFloat(m[1], 64); err == nil {
			// 规整成最短写法：0.10 → 0.1，1.0 → 1
			add(TagRate + ":" + strconv.FormatFloat(f, 'g', -1, 64))
		}
	} else if lowRateRe.MatchString(name) {
		if strings.Contains(name, "低倍率") {
			add(TagRate + ":low")
		} else {
			add(TagRate + ":high")
		}
	}

	// —— 线路特性 ——
	if strings.Contains(name, "流媒体") || strings.Contains(lower, "streaming") {
		add(TagStream)
	}
	if strings.Contains(name, "专线") || strings.Contains(lower, "dedicated") || strings.Contains(lower, "iplc") {
		add(TagDedicated)
	}
	if strings.Contains(lower, "bgp") {
		add(TagBGP)
	}
	if strings.Contains(name, "高速") || strings.Contains(lower, "turbo") {
		add(TagHighSpeed)
	}

	// —— 机场公告节点 ——
	if InfoNodeName(name) {
		add(TagExpiry)
	}

	// —— 协议类别 ——
	if t := strings.ToLower(strings.TrimSpace(nodeType)); t != "" {
		add("proto:" + t)
	}

	return out
}

// unitWords 是不该被当成地区代码的 ASCII 词。
//
// 真实踩到的误报：节点名 "剩余流量：878.1 GB" 里的 GB 是吉字节单位，
// 却被识别成英国（region:GB）。同理 MB/TB/CH 等也要排除。
var unitWords = map[string]bool{
	"gb": true, "mb": true, "tb": true, "kb": true,
	"pb": true, "b": true,
}

// hasRegionKeyword 判断节点名是否含该地区的任一关键词。
//
// 规则：
//   - 中文与 emoji 直接子串匹配（大小写无关）
//   - 拉丁字母关键词要求「整词 + 原样大小写」匹配：
//     整词避免 "in" 命中 "singapore"、"us" 命中 "russia"；
//     大小写敏感避免 "GB"（吉字节）命中 "gb"（英国）。
//     机场里写国家代码时都是大写（HK/US/JP），小写多出现在普通单词里。
func hasRegionKeyword(lower, original string, keys []string) bool {
	for _, k := range keys {
		if k == "" {
			continue
		}
		if isASCIIWord(k) {
			lk := strings.ToLower(k)
			// 单位词永不参与地区判定（GB 是吉字节，不是英国）
			if unitWords[lk] {
				continue
			}
			// 机场写地区代码时习惯大写（HK/US/JP），也常与分隔符连用
			// （US-LosAngeles、HK-01）。这里查大写形式，避免与普通单词冲突。
			if containsASCIIWord(original, strings.ToUpper(k)) {
				return true
			}
			// 多字母长词（hongkong/singapore/losangeles）大小写不限，
			// 但要求 >= 3 字符，避免 us/in 两字母代码误伤普通词
			if len(k) >= 3 && containsWord(lower, lk) {
				return true
			}
			continue
		}
		if strings.Contains(original, k) || strings.Contains(lower, strings.ToLower(k)) {
			return true
		}
	}
	return false
}

// containsASCIIWord 在原串里找「前后不是字母数字」的 word，且大小写完全一致。
func containsASCIIWord(s, word string) bool {
	idx := 0
	for {
		i := strings.Index(s[idx:], word)
		if i < 0 {
			return false
		}
		start := idx + i
		end := start + len(word)
		if (start == 0 || !isAlphaNum(s[start-1])) && (end >= len(s) || !isAlphaNum(s[end])) {
			return true
		}
		idx = start + 1
		if idx >= len(s) {
			return false
		}
	}
}

// isASCIIWord 判断关键词是否为纯 ASCII 字母数字（需要整词匹配）。
func isASCIIWord(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		return false
	}
	return len(s) > 0
}

// containsWord 判断 s 是否含独立的 word（前后不是字母）。
func containsWord(s, word string) bool {
	idx := 0
	for {
		i := strings.Index(s[idx:], word)
		if i < 0 {
			return false
		}
		start := idx + i
		end := start + len(word)
		if (start == 0 || !isAlphaNum(s[start-1])) && (end >= len(s) || !isAlphaNum(s[end])) {
			return true
		}
		idx = start + 1
		if idx >= len(s) {
			return false
		}
	}
}

func isAlphaNum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// HasTag 判断标签集合是否包含给定标签。
func HasTag(tags []string, want string) bool {
	for _, t := range tags {
		if t == want {
			return true
		}
	}
	return false
}

// HasTagPrefix 判断标签集合是否有某一类前缀（如 region: 匹配任意地区）。
func HasTagPrefix(tags []string, prefix string) bool {
	for _, t := range tags {
		if strings.HasPrefix(t, prefix) {
			return true
		}
	}
	return false
}
