package main

import (
	"github.com/gocolly/colly"
	"github.com/PuerkitoBio/goquery"
	"bytes"
	_"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"database/sql"
	"strings"
	"time"
	"golang.org/x/text/encoding/simplifiedchinese"
)

func AddInfo(tableName string, values map[int]interface{},fuc func(err error, value interface{},stmt *sql.Stmt))  {
	db,err := sqlx.Open("mysql","root:wangyang@tcp(localhost:3306)/exchange")
	if err != nil {
		println("open mysql fail")
		return
	}
	defer db.Close()

	// 开启事务
	tx,err := db.Begin()
	if err != nil {
		println(err.Error())
	}

	stmt, err := tx.Prepare(db.Rebind("insert into "+tableName+"(Name" +
		",TradingUnit,BasicCurrency,BuyingRate,CashPurchasePrice,SpotSellingPrice,CashSellingPrice" +
			",BocConversionPrice,ReleaseDate,ReleaseTime,code)" +
				" values (?,?,?,?,?,?,?,?,?,?,?)"))

	if err != nil {
		panic(err)
	}
	for _,value := range values {
		fuc(err,value,stmt)
	}
	err = stmt.Close()
	if err != nil {
		println("stmt close error:", err)
	}

	err = tx.Commit()
	if err != nil {
		println("commit error:", err)
	}
}

// 中国银行mysql封装
func caMysql(err error, value interface{},stmt *sql.Stmt)  {
	newValue := value.(map[int]string)
	var valuesz  [11]string
	valuesz[0] = newValue[0]
	valuesz[1] = "100"
	valuesz[2] = "人民币"
	valuesz[3] = newValue[1]
	valuesz[4] = newValue[2]
	valuesz[5] = newValue[3]
	valuesz[6] = newValue[4]
	valuesz[7] = newValue[5]
	valuesz[8] = newValue[6]
	valuesz[9] = newValue[7]
	valuesz[10] = newValue[8]
	_, err = stmt.Exec(valuesz[0],valuesz[1],valuesz[2],valuesz[3],valuesz[4],valuesz[5],valuesz[6],valuesz[7],valuesz[8],valuesz[9], valuesz[10])
	if err != nil {
		println("Exec error:", err)
	}
}

// 中国银行mysql封装
func jtMysql(err error, value interface{},stmt *sql.Stmt)  {
	newValue := value.(map[int]string)
	var valuesz  [11]string
	valuesz[0] = newValue[0] //货币名称
	valuesz[1] = newValue[1] //
	valuesz[2] = "人民币"     //
	valuesz[3] = newValue[2] // 现汇买入价
	valuesz[4] = newValue[4] // 现钞买入价
	valuesz[5] = newValue[3] // 现汇卖出价
	valuesz[6] = newValue[5] // 现钞卖出价
	valuesz[7] = ""			 // 中行折算价
	valuesz[8] = newValue[6] // 发布日期
	valuesz[9] = newValue[7] // 发布时间
	valuesz[10] = newValue[8]
	_, err = stmt.Exec(valuesz[0],valuesz[1],valuesz[2],valuesz[3],valuesz[4],valuesz[5],valuesz[6],valuesz[7],valuesz[8],valuesz[9],valuesz[10])
	if err != nil {
		println("Exec error:", err)
	}
}

// 中国银行mysql封装
func gsMysql(err error, value interface{},stmt *sql.Stmt)  {
	newValue := value.(map[int]string)
	if newValue[5] == "发布时间"{
		return
	}
	var valuesz  [11]string
	valuesz[0] = newValue[0] //货币名称
	valuesz[1] = "100" //
	valuesz[2] = "人民币"     //
	valuesz[3] = newValue[1] // 现汇买入价
	valuesz[4] = newValue[2] // 现钞买入价
	valuesz[5] = newValue[3] // 现汇卖出价
	valuesz[6] = newValue[4] // 现钞卖出价
	valuesz[7] = ""			 // 中行折算价
	newTimes := strings.Replace(newValue[5],"年","-",-1)
	newTimes = strings.Replace(newTimes,"月","-",-1)
	newTimes = strings.Replace(newTimes,"日","",-1)
	var newTimesData = strings.Split(newTimes," ")
	valuesz[8] = newTimesData[0] // 发布日期
	valuesz[9] = newTimesData[1] // 发布时间
	valuesz[10] = newValue[6]
	_, err = stmt.Exec(valuesz[0],valuesz[1],valuesz[2],valuesz[3],valuesz[4],valuesz[5],valuesz[6],valuesz[7],valuesz[8],valuesz[9],valuesz[10])
	if err != nil {
		println("Exec error:", err)
	}
}

// 中国银行mysql封装
func zsMysql(err error, value interface{},stmt *sql.Stmt)  {
	newValue := value.(map[int]string)
	if newValue[0] == "交易币"{
		return
	}
	var valuesz  [11]string
	valuesz[0] = newValue[0] //货币名称
	valuesz[1] = "100" //
	valuesz[2] = "人民币"     //
	valuesz[3] = newValue[5] // 现汇买入价
	valuesz[4] = newValue[6] // 现钞买入价
	valuesz[5] = newValue[3] // 现汇卖出价
	valuesz[6] = newValue[4] // 现钞卖出价
	valuesz[7] = ""			 // 中行折算价
	var tadyTime = time.Now().Format("2006-01-02")
	valuesz[8] = tadyTime
	valuesz[9] = newValue[7]
	valuesz[10] = newValue[8]
	_, err = stmt.Exec(valuesz[0],valuesz[1],valuesz[2],valuesz[3],valuesz[4],valuesz[5],valuesz[6],valuesz[7],valuesz[8],valuesz[9],valuesz[10])
	if err != nil {
		println("Exec error:", err)
	}
}

// 中国银行mysql封装
func pfMysql(err error, value interface{},stmt *sql.Stmt)  {
	newValue := value.(map[int]string)
	if newValue[1] == "中间价Mid" {
		return
	}
	var valuesz  [11]string
	valuesz[0] = newValue[0] //货币名称
	valuesz[1] = "100" //
	valuesz[2] = "人民币"     //
	valuesz[3] = newValue[2] // 现汇买入价
	valuesz[4] = newValue[3] // 现钞买入价
	valuesz[5] = newValue[4] // 现汇卖出价
	valuesz[6] = newValue[4] // 现钞卖出价
	valuesz[7] = newValue[1] // 中行折算价
	valuesz[8] = newValue[5]
	valuesz[9] = newValue[6]
	valuesz[10] = newValue[7]
	_, err = stmt.Exec(valuesz[0],valuesz[1],valuesz[2],valuesz[3],valuesz[4],valuesz[5],valuesz[6],valuesz[7],valuesz[8],valuesz[9],valuesz[10])
	if err != nil {
		println("Exec error:", err)
	}
}

// 解析中国银行汇率信息
func ca(doc *goquery.Document)  {
	// 爬去所有汇率明细
	tabls := doc.Find("table[align=left]")
	values := make(map[int]interface{})
	tabls.Find("tr").Each(func(i int, selection *goquery.Selection) {
		if i == 0 {
			return
		}
		value := make(map[int]string)
		selection.Find("td").Each(func(i int, selection *goquery.Selection) {
			value[i] = selection.Text()
		})
		name := value[0]
		value[8] = getCode(name)
		values[i] = value
		println(value[0] + " II "+value[8])
	})
	println(values)
	AddInfo("cayh_exchange_info", values, caMysql)
}

// 解析交通银行汇率信息
func jt(doc *goquery.Document)  {
	trs := doc.Find("tr[class=data]")
	times := doc.Find("td[align=left]")
	b := times.Find("b")
	var time1 string
	var time2 string
	b.Each(func(i int, selection *goquery.Selection) {
		var timestr = selection.Text()
		if strings.Contains(timestr, "更新时间"){
			var str = strings.Split(timestr,"：")
			var strs = strings.Split(str[1]," ")
			time1 = strs[0]
			time2 = strs[1]
		}
	})
	values := make(map[int]interface{})
	trs.Each(func(i int, selection *goquery.Selection) {
		value := make(map[int]string)
		selection.Find("td").Each(func(i int, selection *goquery.Selection) {
			value[i] = selection.Text()
		})
		value[6] = time1
		value[7] = time2
		code := string(value[0])
		var star =  UnicodeIndex(code,"(")
		var end = UnicodeIndex(code,"/")
		value[8] = substring(code,star+1,end)
		values[i] = value
	})
	AddInfo("jtyh_exchange_info", values, jtMysql)
}

// 解析工商银行汇率信息
func gs(doc *goquery.Document)  {
	table := doc.Find("table[class=tableDataTable]")
	values := make(map[int]interface{})
	trs := table.Find("tr")
	trs.Each(func(i int, selection *goquery.Selection) {
		tds := selection.Find("td");
		value := make(map[int]string)
		tds.Each(func(i int, selection *goquery.Selection) {
			value[i]= selection.Text()
		})
		code := string(value[0])
		var star =  UnicodeIndex(code,"(")
		var end = UnicodeIndex(code,")")
		value[6] = substring(code,star+1,end)
		values[i] = value
	})
	AddInfo("gsyh_exchange_info",values, gsMysql)
}

// 解析招商银行汇率信息
func zs(doc *goquery.Document)  {
	table := doc.Find("table[class=data]")
	values := make(map[int]interface{})
	trs := table.Find("tr")
	trs.Each(func(i int, selection *goquery.Selection) {
		tds := selection.Find("td");
		value := make(map[int]string)
		tds.Each(func(i int, selection *goquery.Selection) {
			// 去除空格
			str := strings.Replace(selection.Text(), " ", "", -1)
			// 去除换行符
			str = strings.Replace(str, "\n", "", -1)
			value[i] = str
		})
		name := value[0]
		value[8] = getCode(name)
		values[i] = value
		println(value[0] + " II "+value[8])
		values[i]=value
	})
	AddInfo("zsyh_exchange_info",values, zsMysql)
}

// 解析浦发银行息
func pf(doc *goquery.Document)  {
	table := doc.Find("table[class=table_comm]")
	div := doc.Find("table[class=table_comm]+div")
	times := div.Text()
	// 去除换行符
	times = strings.Replace(times, "\n", "", -1)
	times = strings.Replace(times, "\t", "", -1)
	str := strings.Split(times, "：")
	timestat := strings.Split(str[1], " ")
	println(timestat)
	println(str)
	println(times)
	trs := table.Find("tr")
	values := make(map[int]interface{})
	trs.Each(func(i int, selection *goquery.Selection) {
		value := make(map[int]string)
		selection.Find("td").Each(func(i int, selection *goquery.Selection) {
			println(selection.Text())
			value[i] = selection.Text()
		})
		codes := strings.Split(value[0], " ")
		value[5] = timestat[0]
		value[6] = timestat[1]
		if len(codes) > 1 {
			value[7] = codes[1]
			value[0] = codes[0]
		} else {
			value[7] = ""
		}
		values[i] = value
	})
	AddInfo("pfyh_exchange_info", values, pfMysql)
}

// 解析银行代码
func code(doc *goquery.Document) {
	values := make(map[int]interface{})
	span := doc.Find("span[id=huobi]")
	span.Find("a").Each(func(i int, selection *goquery.Selection) {
		title,_ := selection.Attr("title")
		titleByte := []byte(title)
		titleResult,_ := UTF82GB2312(titleByte)
		println(strings.Replace(string(titleResult),"汇率","",-1))
		println(selection.Text())
		value := make(map[int]string)
		value[0] = strings.Replace(string(titleResult),"汇率","",-1)
		value[1] = selection.Text()
		values[i] = value
	})

	db,err := sqlx.Open("mysql","root:wangyang@tcp(localhost:3306)/exchange")
	if err != nil {
		println("open mysql fail")
		return
	}
	defer db.Close()

	// 开启事务
	tx,err := db.Begin()
	if err != nil {
		println(err.Error())
	}

	stmt, err := tx.Prepare(db.Rebind("insert into currency_code(name,code) values (?,?)"))

	if err != nil {
		panic(err)
	}
	for _,value := range values {
		newValue := value.(map[int]string)
		if newValue[0] == "交易币"{
			return
		}
		var valuesz [2]string
		valuesz[0] = newValue[0]
		valuesz[1] = newValue[1]
		_,err := stmt.Exec(valuesz[0],valuesz[1])
		if err != nil {
			println(err.Error())
		}
	}
	err = stmt.Close()
	if err != nil {
		println("stmt close error:", err)
	}

	err = tx.Commit()
	if err != nil {
		println("commit error:", err)
	}
}

/**
	查询code码
 */
func getCode(name string) string  {
	db,err := sqlx.Open("mysql","root:wangyang@tcp(localhost:3306)/exchange")
	if err != nil {
		println("open mysql fail")
		return ""
	}
	defer db.Close()
	rows, err := db.Query("select code from currency_code where name like '%"+name+"%'")

	var code string
	for rows.Next() {
		err := rows.Scan(&code)
		if err != nil {
			println(err)
		}
	}
	rows.Close()
	if err != nil {
		panic(err)
	}
	if err != nil {
		println("stmt close error:", err)
	}
	return code
}

// 封装请求头信息 以及请求信息
func getHtml(url string, allUrl string, fuc func(*goquery.Document))  {
	var stockList string
	var err error
	c := colly.NewCollector()
	c.UserAgent = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36"
	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("Host", "query.sse.com.cn")
		r.Headers.Set("Connection", "keep-alive")
		r.Headers.Set("Accept", "*/*")
		r.Headers.Set("Origin", url)
		r.Headers.Set("Referer", allUrl) //关键头 如果没有 则返回 错误
		r.Headers.Set("Accept-Encoding", "gzip, deflate")
		r.Headers.Set("Accept-Language", "zh-CN,zh;q=0.9")
	})
	c.OnResponse(func(resp *colly.Response) {
		stockList = string(resp.Body)
		var html = bytes.NewReader(resp.Body)
		doc,_ := goquery.NewDocumentFromReader(html)
		fuc(doc)
	})
	c.OnError(func(resp *colly.Response, errHttp error) {
		err = errHttp
		println(err.Error())
	})
	err = c.Visit(allUrl)
}

func main()  {

	// 解析银行代码
	//getHtml("http://www.webmasterhome.cn","http://www.webmasterhome.cn/huilv/huobidaima.asp",code)

	// 获取中国银行汇率信息
	getHtml("http://www.boc.cn","http://www.boc.cn/sourcedb/whpj/", ca)

	// 获取交通银行汇率信息
	getHtml("http://www.bankcomm.com", "http://www.bankcomm.com/BankCommSite/simple/cn/whpj/queryExchangeResult.do?type=simple", jt)

	// 获得获取工商银行汇率信息
	getHtml("http://www.icbc.com.cn","http://www.icbc.com.cn/ICBCDynamicSite/Optimize/Quotation/QuotationListIframe.aspx", gs)

	// 获取招商银行汇率信息
	getHtml("http://fx.cmbchina.com","http://fx.cmbchina.com/hq/",zs)

	// 获取浦发银行汇率信息
	getHtml("http://ebank.spdb.com.cn","http://ebank.spdb.com.cn/net/QueryExchangeRate.do",pf)

}
func UTF82GB2312(s []byte)([]byte, error) {
	var decodeBytes,_=simplifiedchinese.GB18030.NewDecoder().Bytes(s)
	return decodeBytes, nil
}

//start:开始index，从0开始，包括0
//end:结束index，以end结束，但不包括end
func substring(source string, start int, end int) string {
	var r = []rune(source)
	length := len(r)

	if start < 0 || end > length || start > end {
		return ""
	}

	if start == 0 && end == length {
		return source
	}

	return string(r[start : end])
}

func UnicodeIndex(str,substr string) int {
	// 子串在字符串的字节位置
	result := strings.Index(str,substr)
	if result >= 0 {
		// 获得子串之前的字符串并转换成[]byte
		prefix := []byte(str)[0:result]
		// 将子串之前的字符串转换成[]rune
		rs := []rune(string(prefix))
		// 获得子串之前的字符串的长度，便是子串在字符串的字符位置
		result = len(rs)
	}

	return result
}
