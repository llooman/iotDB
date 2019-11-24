/*

	{"cmd":"graphs"}
	{"cmd":"timedata"}
	{"cmd":"timedataDiff"}
iotCommand
	go get -u github.com/go-sql-driver/mysql

	0.0.0.0 tcp4 = IPv4 wildcard address

	https://thewhitetulip.gitbooks.io/webapp-with-golang-anti-textbook/content/manuscript/2.2database.html
	- Avoiding Prepared Statements
	- Prepared Statements in Transactions
	http://go-database-sql.org/prepared.html
	Because statements will be re-prepared as needed when their original connection is busy,
	it’s possible for high-concurrency usage of the database,
	which may keep a lot of connections busy, to create a large number of prepared statements.
	This can result in apparent leaks of statements, statements being prepared and re-prepared
	more often than you think, and even running into server-side limits on the number of statements.

	https://en.wikipedia.org/wiki/Time_series
	dataPoints
	timePoints
*/

package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"iot"
	"log"
	"math"
	"net"
	"os"

	// "strconv"

	// "path"
	"bufio"
	"encoding/json"
	"runtime"
	"time"

	"database/sql"
	"strconv"
	"strings"

	"github.com/tkanos/gonfig"

	_ "github.com/go-sql-driver/mysql"
)

type DataRequest struct {
	Cmd    string
	Id     int64
	Start  int64
	Stop   int64
	Length int64
}

type iotColumn struct {
	Name    string
	ColType string
}

type iotKey struct {
	Name   string
	Create string
}

type iotTable struct {
	Name    string
	Columns []iotColumn
	PKey    string
	Engine  string
}

var dbJson string

var database *sql.DB
var databaseNew *sql.DB

var persistLogRunning bool

var logOptions map[int]int

var dbCheckSum string

var (
	Detail  *log.Logger
	Trace   *log.Logger
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
)

type IotConfig struct {
	Port        string
	Database    string
	DatabaseNew string
	Debug       bool
}

var iotConfig IotConfig

var timeFromOldDB bool

func main() {

	InitLogging(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)

	err := gonfig.GetConf(iot.Config(), &iotConfig)
	checkError(err)

	if iotConfig.Debug {
		InitLogging(os.Stdout, os.Stdout, os.Stdout, os.Stderr)
		Trace.Print("Debug ...")
	}

	//	if len(os.Args) != 2 {
	//        fmt.Fprintf(os.Stderr, "Usage: %s ip-addr\n", os.Args[0])
	//        os.Exit(1)
	//    }

	database, err = sql.Open("mysql", iotConfig.Database)
	checkError(err)
	defer database.Close()

	databaseNew, err = sql.Open("mysql", iotConfig.DatabaseNew)
	checkError(err)
	defer databaseNew.Close()

	dataDefinition()

	service := "0.0.0.0:" + iotConfig.Port
	if runtime.GOOS == "windows" {
		service = "localhost:" + iotConfig.Port
	}
	err = startDbService(service)
	checkError(err)

	//	go persistLogScheduler()

	logOptions = make(map[int]int) // not used ???

	Info.Printf("%s started", service)

	for true {
		time.Sleep(49 * time.Second) //49

		// fmt.Printf(`persistMemTablesRunning %d`, persistMemTablesRunning)

		err = database.Ping()
		checkError(err)

		err = databaseNew.Ping()
		checkError(err)
	}

	Warning.Printf("stop %s", service)
}

func handleDbServiceRequest(conn net.Conn) {

	//TODO prevent long request attack

	buff := make([]byte, 1024)

	var cmd = "startServiceLoop"
	var nulByte = make([]byte, 0)
	var payload string
	var iotPayload iot.IotPayload

	defer conn.Close() // close connection before exit

	for cmd != "stop" {

		var respBuff bytes.Buffer

		conn.SetReadDeadline(time.Now().Add(49 * time.Second))

		n, err := conn.Read(buff)

		payload = strings.TrimSpace(string(buff[0:n]))
		iotPayload = iot.ToPayload(payload)

		Info.Printf("payload:%s\n", payload)
		Trace.Printf("iotPayload:%+v\n", iotPayload)

		if err != nil {

			if err.Error() == "EOF" {
				return
			}

			fmt.Println("idb read err:", err.Error())
			return
		}

		// 	// timedata","id":552,"start":1570226400,"stop":1570312800
		// 	// timedata&552&1570226400&1570312800
		// 	//              1570226400
		// }

		if iotPayload.Cmd == "timedata" {
			timedata(&respBuff, &iotPayload)
			conn.Write(respBuff.Bytes())
			Detail.Printf("%s\n\n", respBuff.String())

		} else if iotPayload.Cmd == "timedataDiff" {
			timedataDiff(&respBuff, &iotPayload)
			conn.Write(respBuff.Bytes())
			Detail.Printf("%s\n\n", respBuff.String())

		} else if iotPayload.Cmd == "timedataDeltas" {
			timedataDeltas(&respBuff, &iotPayload)
			conn.Write(respBuff.Bytes())
			Detail.Printf("%s\n\n", respBuff.String())

		} else if iotPayload.Cmd == "graphPlans" {
			graphPlans(&respBuff)
			conn.Write(respBuff.Bytes())
			Detail.Printf("%s\n\n", respBuff.String())

		} else if iotPayload.Cmd == "savePlans" {
			savePlans(&respBuff, &iotPayload)
			conn.Write(respBuff.Bytes())

		} else if iotPayload.Cmd == "graphs" {
			graphs(&respBuff)
			Detail.Printf("%s\n\n", respBuff.String())
			conn.Write(respBuff.Bytes())

		} else if iotPayload.Cmd == "graphs2" {
			graphs2(&respBuff)
			Detail.Printf("%s\n\n", respBuff.String())
			conn.Write(respBuff.Bytes())

		} else if iotPayload.Cmd == "ping" {
			conn.Write([]byte(fmt.Sprintf(`{"retcode":0,"message":"pong"}`)))

		} else if iotPayload.Cmd == "close" || cmd == "stop" {
			conn.Write(nulByte)

		} else {
			conn.Write([]byte(fmt.Sprintf(`{"retcode":99,"message":"cmd %s not found"}`, iotPayload.Cmd)))
		}

	}
}

func dataDefinition() { // TODO check   keys

	var dbDef []iotTable

	err2 := json.Unmarshal([]byte(dbJson), &dbDef)
	if err2 != nil {
		fmt.Println("error:", err2)
		os.Exit(1)
	}

	for _, tbl := range dbDef {

		if tableNotExists(tbl.Name) {
			createTable(tbl)

		} else {
			// fmt.Printf("exists %+v  \n", tbl)
			// fmt.Printf("exists %s  \n", tbl.Name)
			for _, col := range tbl.Columns {
				checkColumn(tbl.Name, col)

			}
		}
	}

	refreshViews()
	refreshProcs()
}

func refreshViews() {

	sql := `CREATE OR REPLACE VIEW vwTrace AS 
			SELECT (to_days(now()) - to_days(from_unixtime(timestamp))) AS dd 
			, date_format(from_unixtime(timestamp)
			, '%H:%i:%s') AS at 
			, timestamp AS ts
			, van AS f
			, code AS id
			, val AS v
			, errLevel
			, msg FROM domoTraceMem 
			UNION 
			SELECT (to_days(now()) - to_days(from_unixtime(timestamp))) AS dd 
			, date_format(from_unixtime(timestamp),'%H:%i:%s') AS at 
			, timestamp AS ts
			, van AS f
			, code AS id
			, val AS v
			, errLevel
			, msg FROM domoTrace`

	_, err := databaseNew.Query(sql)
	if err != nil {
		fmt.Printf("refreshViews: %v", err.Error())
	}
}

func refreshProcs() {

	sql := "DROP PROCEDURE IF EXISTS saveMem"

	_, err := databaseNew.Query(sql)

	if err != nil {
		fmt.Printf("refreshProcs: %v", err.Error())
	}

	sql = `
	CREATE PROCEDURE saveMem ()
		
	BEGIN		
	DECLARE myCount bigint(9); 

	SET @sql0 = 'SELECT count(*)  INTO @myCount from nodesMem'; 
	PREPARE stmt0 FROM @sql0;  EXECUTE stmt0;  DEALLOCATE PREPARE stmt0; 

	IF ( @myCount > 0 ) THEN 
		delete from nodesDsk; 
		insert into nodesDsk select * from nodesMem; 
	ELSE 
		insert into nodesMem select * from nodesDsk; 
	END IF; 

	SET @sql1 = 'SELECT count(*)  INTO @myCount from propsMem'; 
	PREPARE stmt1 FROM @sql1;  EXECUTE stmt1;  DEALLOCATE PREPARE stmt1; 

	IF ( @myCount > 0 ) THEN 
		delete from propsDsk; 
		insert into propsDsk select * from propsMem; 
	ELSE 
		insert into propsMem select * from propsDsk; 
	END IF;

	END`

	_, err = databaseNew.Query(sql)

	if err != nil {
		fmt.Printf("refreshProcs: %v", err.Error())
	}
}

func createTable(table iotTable) {

	sql := fmt.Sprintf("CREATE TABLE %s (", table.Name)

	for _, col := range table.Columns {
		sql += fmt.Sprintf("%s %s,", col.Name, col.ColType)
	}

	sql += fmt.Sprintf(" primary key (%s)) ENGINE=%s;", table.PKey, table.Engine)

	fmt.Println(sql)

	_, err := databaseNew.Exec(sql)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func checkColumn(table string, column iotColumn) {
	var Field string
	var ColumnType string
	var Null string
	var Key sql.NullString
	var Default sql.NullString
	var Extra sql.NullString

	// var decimalFactor sql.NullInt64

	columnRow, errr := databaseNew.Query(fmt.Sprintf("SHOW COLUMNS from %s like '%s'", table, column.Name))
	if errr != nil {
		log.Fatal(errr)
	}
	defer columnRow.Close()

	if columnRow.Next() {
		// fmt.Printf("exists %+v  \n", tbl)
		columnRow.Scan(&Field, &ColumnType, &Null, &Key, &Default, &Extra)
		//           	Field|  Type       | Null | Key | Default | Extra
		// fmt.Printf("%s: %s %s, null:%s, key:%s, default:%s, extra:%s\n", table, Field, ColumnType, Null, Key.String, Default.String, Extra.String)

		current := ColumnType
		if Null == "NO" {
			if Default.String == "" {
				current += fmt.Sprintf(" NOT NULL")

			} else if strings.HasPrefix(strings.ToUpper(ColumnType), "VARCHAR") {
				current += fmt.Sprintf(" NOT NULL DEFAULT '%s'", Default.String)

			} else {
				current += fmt.Sprintf(" NOT NULL DEFAULT %s", Default.String)
			}
		} else {
			if Default.String == "" {
				current += fmt.Sprintf(" DEFAULT NULL")

			} else {
				current += fmt.Sprintf(" NULL ?? DEFAULT %s", Default.String)

			}
		}

		if column.ColType == "DELETE" {
			fmt.Printf("dropColumn %s %s\n", column.Name, column.ColType)
			dropColumn, errr := databaseNew.Query(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", table, column.Name))
			if errr != nil {
				log.Fatal(errr)
			}
			defer dropColumn.Close()

		} else if strings.ToUpper(current) != strings.ToUpper(column.ColType) {
			fmt.Printf("modifyColumn %s |%s>%s|\n", column.Name, current, column.ColType)
			modifyColumn, errr := databaseNew.Query(fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s", table, column.Name, column.ColType))
			if errr != nil {
				log.Fatal(errr)
			}
			defer modifyColumn.Close()

		}

	} else if column.ColType != "DELETE" {
		fmt.Printf("addColumn %s %s\n", column.Name, column.ColType)
		addColumn, errr := databaseNew.Query(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column.Name, column.ColType))
		if errr != nil {
			log.Fatal(errr)
		}
		defer addColumn.Close()
	}
}

func tableNotExists(table string) bool {

	tableRow, errr := databaseNew.Query(fmt.Sprintf("SHOW TABLES LIKE '%s'", table))
	if errr != nil {
		log.Fatal(errr)
	}
	defer tableRow.Close()

	if tableRow.Next() {
		return false
	}

	return true
}

func persistLogScheduler() {
	fmt.Println("persistLog( )")
	for true {
		persistLog()
		time.Sleep(1 * time.Hour)
	}
}

func getLogOption(varID int) int {

	if logOpt, ok := logOptions[varID]; ok {
		return logOpt

	} else {

		var logOpt sql.NullInt64

		propQuery, errr := databaseNew.Prepare(`SELECT logOption FROM iotprops where varid = ?`)
		checkError(errr)
		defer propQuery.Close()

		propRows, errr := propQuery.Query(varID) // nowTimeStamp
		if errr != nil {
			log.Fatal(errr)
		}
		defer propRows.Close()

		if propRows.Next() {

			err := propRows.Scan(&logOpt)
			checkError(err)

			if logOpt.Valid {
				logOptions[varID] = int(logOpt.Int64)
				return logOptions[varID]
			}

		} else {
			fmt.Printf("getLogOption varID:%d not found!", varID)
		}

		return 0
	}
}

func persistLog() {

	persistLogRunning = true

	nowTimeStamp := time.Now().Unix()

	// diskUpdate, err := databaseNew.Prepare(`
	// INSERT INTO IoTValuesDisk(id, ist, istTimer, nextRefresh, error, retryCount) VALUES(?, ?, ?, ?, ?, ?)
	// ON DUPLICATE KEY UPDATE ist=?, istTimer=?, nextRefresh=?, error=?, retryCount=? `)

	// checkError(err)

	// if err != nil {
	// 	panic(err.Error())
	// }

	memQuery, errr := database.Prepare(`
	SELECT timestamp, id, val, delta FROM IoTValuesMem
	where not val is null and timestamp <= ? and id >= 100 ORDER BY id, timestamp DESC
	`)

	checkError(errr)

	defer memQuery.Close()

	var timestamp sql.NullInt64
	var id sql.NullInt64
	var val sql.NullInt64
	var delta sql.NullInt64

	// rows, errr := stmt.Query(req.Id, req.Start, req.Id, req.Start, req.Stop, req.Id, req.Start, req.Id, req.Start, req.Stop)
	memRows, errr := memQuery.Query(nowTimeStamp) // nowTimeStamp
	// if errr != nil {
	// 	log.Fatal(errr)
	// }
	checkError(errr)

	defer memRows.Close()

	//	      	int logType   = 0;
	nextID := 0
	currID := 0
	currCount := 0
	skipCount := 2 // number of values we skip to decide which can be cleaned

	var timestamps [3]int64
	var values [3]int64
	var deltas [3]int64

	insertTheMiddleValue := false

	test := currID + currCount + skipCount
	test = test

	// IotSensor var = null;
	// OptionAdd currAddOption=OptionAdd.none;
	// OptionAdd nextAddOption=OptionAdd.none;

	for memRows.Next() {

		err := memRows.Scan(&timestamp, &id, &val, &delta)

		checkError(err)

		iot.SqlInt(&nextID, id)
		values[2] = values[1]
		values[1] = values[0]
		iot.SqlInt64(&values[0], val)
		timestamps[2] = timestamps[1]
		timestamps[1] = timestamps[0]
		iot.SqlInt64(&timestamps[0], timestamp)
		deltas[2] = deltas[1]
		deltas[1] = deltas[0]
		iot.SqlInt64(&deltas[0], delta)

		//logger.info("migrateDomoLog id "+nextId+" timestamp " + timestamps[0] + " val  "+ values[0]+" currCount " + currCount );
		fmt.Printf("next id:%d val:%d\n", currID, values[0])

		if nextID == currID {

			if currCount <= skipCount {
				currCount++
			} else {
				// logType 1 = always insert, 2 = skip when equal, 3 also skip using extraPolate
				insertTheMiddleValue = true
				insertTheMiddleValue = insertTheMiddleValue

				logOpt := getLogOption(currID)

				if logOpt == 1 || logOpt == 2 { //  skipEqualWithPrevious || extrapolate

					if values[0] == values[1] && values[1] == values[2] {
						insertTheMiddleValue = false
					}
				}

				if logOpt == 2 && insertTheMiddleValue { //  extrapolate

					// if extrapolate 1 and 2 to 3. When 3 comes close then skip 2=the middle one
					partialTimeSpan := float64(timestamps[1] - timestamps[0])
					totalTimeSpan := float64(timestamps[2] - timestamps[0])
					partialDelta := float64(values[1] - values[0])
					realDelta := float64(values[2] - values[0])
					calculatedDelta := partialDelta * totalTimeSpan / partialTimeSpan
					if math.Abs(realDelta-calculatedDelta) < 0.6 {
						insertTheMiddleValue = false
					}
					//						logger.info("migrateDomoLog deltaTemp1="+deltaTemp1+" deltaTempTot="+deltaTempTot+" deltaTime1="+deltaTime1+" deltaTimeTot="+deltaTimeTot+" targetTemp="+targetTemp+" skip="+skip );
				}

				if insertTheMiddleValue {
					fmt.Printf("insert middel id:%d \n", currID)

					//TODO	diskUpdate.Exec(prop.VarId, prop.NextRefresh, prop.IsError, prop.RetryCount, nextRefresh, prop.IsError, prop.RetryCount)

					// //logger.info("migrateDomoLog ! skip");
					// rsDisk.moveToInsertRow();									// copy the row from memory table to disk table
					// rsDisk.updateLong("timestamp", timestamps[1] );
					// rsDisk.updateInt("id", currId );
					// rsDisk.updateLong("val", values[1] );
					// rsDisk.updateLong("delta", delta[1] );
					// try{rsDisk.insertRow();} catch(Exception ex){
					// 	logger.error("migrateDataLog2Disk2: insertMiddle ex="+ ex.getMessage());
					// }

				} else {
					values[1] = values[2]
					timestamps[1] = timestamps[2]
					deltas[1] = deltas[2]
				}

				fmt.Printf("Nullify middel id:%d \n", currID)
				// 	  //logger.info("Nullify previous");
				// 	  try {
				// 	  rsMemory.previous();
				// 	  rsMemory.updateNull("val" ); 	// nullify mem table
				// 	  rsMemory.updateRow();
				// 	  rsMemory.next();
				// 	  } catch(Exception ex){
				// 		  logger.error("migrateDataLog2Disk2: nullify previous ex="+ ex.getMessage());
				// 	  }

			}

		} else {

			//			  prop := getSensor(nextId);
			//
			// 			if( var == null )
			// 			{
			// //			      		throw new Exception("Id "+nextId+" not found in variables !");
			// 				logger.error("Id "+nextId+" not found in variables !");
			// 			}
			// 			else
			// 			{
			// 			  nextAddOption = var.addOption;
			// 			  //logger.info("migrateDomoLog start id "+nextId+" addOption " + nextAddOption );

			if currID != 0 { //&& currCount > 1

				fmt.Printf("insert last id:%d \n", currID)
				//logger.info("migrateDomoLog migrate last id "+currId  );
				// always migrate last timestamp (prev id)

				//   rsDisk.moveToInsertRow();									// copy the row from memory table to disk table
				//   rsDisk.updateLong("timestamp", timestamps[1] );
				//   rsDisk.updateInt("id", currId );
				//   rsDisk.updateLong("val", values[1] );
				//   rsDisk.updateLong("delta", delta[1] );
				//   try{rsDisk.insertRow();} catch(Exception ex){
				// 	  logger.error("migrateDataLog2Disk2: insertCurrLast ex="+ ex.getMessage());
				//   }

				// clean mem on first id
				//logger.info("Nullify last");
				fmt.Printf("nullify last id:%d \n", currID)
				//   try {
				//   rsMemory.previous();
				//   rsMemory.updateNull(3); 	// nullify mem table
				//   rsMemory.updateRow();
				//   rsMemory.next();
				//   } catch(Exception ex){
				// 	  logger.error("migrateDataLog2Disk2: nullify last ex="+ ex.getMessage());
				//   }

			}

			currID = nextID
			// always add first
			fmt.Printf("insert first id:%d\n", currID)
			// //logger.info("Insert first mem id "+currId);
			// rsDisk.moveToInsertRow();									// copy the row from memory table to disk table
			// rsDisk.updateLong("timestamp", timestamps[0] );
			// rsDisk.updateInt("id", currId );
			// rsDisk.updateLong("val", values[0] );
			// rsDisk.updateLong("delta", delta[0] );
			// try{
			// 	rsDisk.insertRow();

			// } catch(Exception ex){
			// 	logger.error("migrateDataLog2Disk2: insertfirst ex="+ ex.getMessage());
			// }
			fmt.Printf("nullify first id:%d\n", currID)
			// 			try {
			// 				rsMemory.updateNull("val" ); 	// nullify mem table
			// 				rsMemory.updateRow();
			// 				} catch(Exception ex){
			// 					//logger.error("migrateDataLog2Disk2: nullify first id="+currId+" ex="+ ex.getMessage());
			// 				}

			// 				if(var.dataType == OptionDataType.Bool)
			// 				{
			// 					var.skipSkipEqualWithPrevious = true;
			// 					// ????   prevent error in setVal with skipEqualWithPrevious
			// //						var.prevIst = -1;
			// //						var.ist=-1;
			// 				}
		}

		// logType 1 = always insert, 2 = skip when equal, 3 also skip using extraPolate

		// 	cnt++

		// 	var nextRefresh int64
		// 	if prop.RefreshRate > 0 {
		// 		nextRefresh = time.Now().Unix() + int64(prop.RefreshRate)
		// }

		// 	insForm.Exec(prop.VarId, prop.Ist, prop.IstTimeStamp, nextRefresh, prop.IsError, prop.RetryCount, prop.Ist, prop.IstTimeStamp, nextRefresh, prop.IsError, prop.RetryCount)
	}

	// processing after the last found
	if currCount > 1 {
		fmt.Printf("process EOF id:%d\n", currID)
	}

	persistLogRunning = false
}

//target.SetReadDeadline(time.Now().Add(49 * time.Second))

//----------------------------------------------------------------------------------------
func startDbService(service string) error {

	var err error
	var tcpAddr *net.TCPAddr

	tcpAddr, err = net.ResolveTCPAddr("tcp4", service)
	if err != nil {
		return err
	}
	//    checkError(err)

	tcpListener, errr := net.ListenTCP("tcp4", tcpAddr)
	if errr != nil {
		return errr
	}
	//    checkError(err)

	// fmt.Println("DbService: " + service  )
	// fmt.Printf("idb: %s \n", service)

	go dbServiceListener(tcpListener)

	return err
}

func dbServiceListener(listener *net.TCPListener) {

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		// multi threading:
		go handleDbServiceRequest(conn)
	}
}

func savePlans(respBuff *bytes.Buffer, iotPayload *iot.IotPayload) {

	var err error
	name := iotPayload.Parms[1]
	ids := iotPayload.Parms[2]

	_, err = databaseNew.Query(fmt.Sprintf(`delete from graphPlans where name = '%s'`, name))
	checkError(err)

	if ids != "" {

		idsArr := strings.Split(ids, "_")

		for _, id := range idsArr {

			sql := fmt.Sprintf(`insert into graphPlans (id, name) values (%s,'%s')`, id, name)
			// Trace.Printf(sql)

			_, err = databaseNew.Query(sql)
			checkError(err)

		}
	}
	respBuff.Write([]byte(`{"retCode":0}`))

	// {\"retCode\":0,\"message\":\"\"}
}

func graphPlans(respBuff *bytes.Buffer) {

	var err error

	stmt, errr := databaseNew.Prepare("Select id, name from graphPlans  order by name, id")

	checkError(errr)

	defer stmt.Close()

	rows, errr := stmt.Query()
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	respBuff.Write([]byte(fmt.Sprintf(`{"graphPlans":[`)))

	cnt := 0

	var id int
	var nextName string
	currName := ""

	var ids bytes.Buffer

	for rows.Next() {
		err := rows.Scan(&id, &nextName)
		checkError(err)

		cnt++

		if currName == "" ||
			currName != nextName {

			if currName != "" {
				respBuff.Write([]byte(fmt.Sprintf("%s]}", ids.String())))
			}
			if cnt > 1 {
				respBuff.Write([]byte(","))
			}

			respBuff.Write([]byte(fmt.Sprintf(`{"graphPlan":"%s","ids":[`, nextName)))

			ids.Reset()
			ids.Write([]byte(strconv.Itoa(id)))
		} else {
			ids.Write([]byte(fmt.Sprintf(`,%s`, strconv.Itoa(id))))
		}
		currName = nextName
	}

	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	if respBuff.Len() > 0 {
		respBuff.Write([]byte(fmt.Sprintf(`%s]}`, ids.String())))
	}

	respBuff.Write([]byte("]}\n"))

	//fmt.Printf("graphPlans: %s \n", respBuff.String())

	err = stmt.Close()
	checkError(err)
}

func graphs(respBuff *bytes.Buffer) {

	var err error

	stmt, errr := database.Prepare(`
	  Select id, name, drawType, drawColor, drawOffset, drawFactor
	  from IoTSensors where drawType > 0 order by id`)

	checkError(errr)

	defer stmt.Close()

	rows, errr := stmt.Query()
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	//{"graphs":[{"id":113,"name":"connMem","factor":0.020,"type":1,"color":"black"},{"id":213,
	respBuff.Write([]byte(fmt.Sprintf(`{"graphPlan":"Pomp","graphs":[`)))
	// fmt.Printf(`{"id":%d,"from":%d\n`, req.Id, req.Start)

	cnt := 0

	var id int
	var name string
	var drawtype int64
	var drawtypeSql sql.NullInt64
	var color string
	var drawOffset float64
	var drawFactor float64
	var drawOffsetSql sql.NullFloat64
	var drawFactorSql sql.NullFloat64

	for rows.Next() {
		err := rows.Scan(&id, &name, &drawtypeSql, &color, &drawOffsetSql, &drawFactorSql)
		checkError(err)

		cnt++

		if cnt == 1 {
			// startTimestamp = timestamp
		}

		if cnt > 1 {
			respBuff.Write([]byte(`,`))
		}

		if drawtype = drawtypeSql.Int64; !drawtypeSql.Valid {
			drawtype = 0
		}

		if drawOffset = drawOffsetSql.Float64; !drawOffsetSql.Valid {
			drawOffset = 0
		}

		if drawFactor = drawFactorSql.Float64; !drawFactorSql.Valid {
			drawFactor = 0
		}

		respBuff.Write([]byte(fmt.Sprintf(`{"id":%d,"name":"%s","type":%d,"color":"%s","drawOffset":%f,"drawFactor":%f}`, id, name, drawtype, color, drawOffset, drawFactor)))

	}

	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	// conn.Write([]byte(fmt.Sprintf(`],"count":%d,"start":%d,"stop":%d,"stopval":%d}`, cnt, startTimestamp, timestamp, val)))

	// fmt.Printf(`timedata: ],"count":%d,"start":%d,"stop":%d,"stopval":%d}\n\n`, cnt, startTimestamp, timestamp, val)

	respBuff.Write([]byte("]}\n"))

	err = rows.Close()
	checkError(err)

	if errr := rows.Err(); errr != nil {
		checkError(errr)
	}

	err = stmt.Close()
	checkError(err)
}

func graphs2(respBuff *bytes.Buffer) {

	var err error

	stmt, errr := databaseNew.Prepare(`
	  Select varId, nodeId, propId, name, drawType, drawColor, drawOffset, drawFactor
	  from propsdef where drawType > 0 order by varid`)

	checkError(errr)

	defer stmt.Close()

	rows, errr := stmt.Query()
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	//{"graphs":[{"id":113,"name":"connMem","factor":0.020,"type":1,"color":"black"},{"id":213,
	respBuff.Write([]byte(fmt.Sprintf(`{"graphPlan":"Pomp","graphs":[`)))

	cnt := 0

	var varid int
	var nodeid int
	var propid int
	var name string
	var drawtype int64
	var drawtypeSql sql.NullInt64
	var color string
	var drawOffset float64
	var drawFactor float64
	var drawOffsetSql sql.NullFloat64
	var drawFactorSql sql.NullFloat64

	for rows.Next() {
		err := rows.Scan(&varid, &nodeid, &propid, &name, &drawtypeSql, &color, &drawOffsetSql, &drawFactorSql)
		checkError(err)

		cnt++

		if cnt == 1 {
			// startTimestamp = timestamp
		}

		if cnt > 1 {
			respBuff.Write([]byte(`,`))
		}

		if drawtype = drawtypeSql.Int64; !drawtypeSql.Valid {
			drawtype = 0
		}

		if drawOffset = drawOffsetSql.Float64; !drawOffsetSql.Valid {
			drawOffset = 0
		}

		if drawFactor = drawFactorSql.Float64; !drawFactorSql.Valid {
			drawFactor = 0
		}

		respBuff.Write([]byte(fmt.Sprintf(`{"varId":%d,"nodeId":%d,"propId":%d,"name":"%s","type":%d,"color":"%s","drawOffset":%f,"drawFactor":%f}`, varid, nodeid, propid, name, drawtype, color, drawOffset, drawFactor)))

	}

	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	respBuff.Write([]byte("]}\n"))

	err = rows.Close()
	checkError(err)

	if errr := rows.Err(); errr != nil {
		checkError(errr)
	}

	err = stmt.Close()
	checkError(err)
}

func timedata(respBuff *bytes.Buffer, iotPayload *iot.IotPayload) {

	var err error

	/*
	*  combine all values from domoLog and domoLogMem
	*  find 1 more values just over the edge.
	*  ! duplicates are automatically removed by the Distinct
	 */
	stmt, errr := database.Prepare(`
	Select Distinct * from
	(  	  ( SELECT timestamp, val from domoLog    WHERE id = ? and not val is null and timestamp <= ?                   ORDER BY timestamp DESC LIMIT 0,1 )
	Union ( SELECT timestamp, val from domoLog    WHERE id = ? and not val is null and timestamp > ? and timestamp <= ? ORDER BY timestamp )
	Union ( SELECT timestamp, val from domoLogMem WHERE id = ? and not val is null and timestamp <= ?                   ORDER BY timestamp DESC LIMIT 0,1 )
	Union ( SELECT timestamp, val from domoLogMem WHERE id = ? and not val is null and timestamp > ? and timestamp <= ? ORDER BY timestamp )
	) as result ORDER BY timestamp`)

	checkError(errr)

	defer stmt.Close()

	idFactor := iot.IDFactor

	if timeFromOldDB {
		idFactor = 100
	}

	varId := iotPayload.NodeId*idFactor + iotPayload.PropId

	rows, errr := stmt.Query(
		varId, iotPayload.Start,
		varId, iotPayload.Start, iotPayload.Stop,
		varId, iotPayload.Start,
		varId, iotPayload.Start, iotPayload.Stop)
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	var startTimestamp = 0
	var timestamp = 0
	var val = 0
	var prevTimestamp = 0
	var cnt = 0

	respBuff.Write([]byte(fmt.Sprintf(`{"id":%d,"from":%d,"timedata":[`, iotPayload.VarId, iotPayload.Start)))
	// fmt.Printf(`{"id":%d,"from":%d\n`, req.Id, req.Start)

	for rows.Next() {
		err := rows.Scan(&timestamp, &val)
		checkError(err)

		cnt++

		if cnt == 1 {
			startTimestamp = timestamp
		}

		var deltaTijd = timestamp - prevTimestamp

		if cnt > 1 {
			respBuff.Write([]byte(`,`))
		}
		respBuff.Write([]byte(fmt.Sprintf("%d,%d", deltaTijd, val)))
		if cnt > 825 {
			// fmt.Printf("[%d,%d,%d]\n", cnt, prevTimestamp, timestamp)
			// fmt.Printf("[%d,%d,%d]\n", cnt, deltaTijd, deltaVal)
		}

		prevTimestamp = timestamp
	}

	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	respBuff.Write([]byte(fmt.Sprintf(`],"count":%d,"start":%d,"stop":%d,"stopval":%d}`, cnt, startTimestamp, timestamp, val)))

	// fmt.Printf(`timedata: ],"count":%d,"start":%d,"stop":%d,"stopval":%d}\n\n`, cnt, startTimestamp, timestamp, val)

	respBuff.Write([]byte("\n"))

	err = rows.Close()
	checkError(err)

	if errr := rows.Err(); errr != nil {
		checkError(errr)
	}

	err = stmt.Close()
	checkError(err)
}

func timedataDeltas(respBuff *bytes.Buffer, iotPayload *iot.IotPayload) {

	var err error

	/* return delta i.p.v. val
	*  combine all values from domoLog and domoLogMem
	*  find 1 more values just over the edge.
	*  ! duplicates are automatically removed by the Distinct
	 */
	stmt, errr := database.Prepare(`
	Select Distinct * from
	(  	  ( SELECT timestamp, delta from domoLog    WHERE id = ? and  delta > 0 and timestamp <= ?                   ORDER BY timestamp DESC LIMIT 0,1 )
	Union ( SELECT timestamp, delta from domoLog    WHERE id = ? and  delta > 0 and timestamp > ? and timestamp <= ? ORDER BY timestamp )
	Union ( SELECT timestamp, delta from domoLogMem WHERE id = ? and  delta > 0 and timestamp <= ?                   ORDER BY timestamp DESC LIMIT 0,1 )
	Union ( SELECT timestamp, delta from domoLogMem WHERE id = ? and  delta > 0 and timestamp > ? and timestamp <= ? ORDER BY timestamp )
	) as result ORDER BY timestamp`)

	checkError(errr)

	defer stmt.Close()

	idFactor := iot.IDFactor

	if timeFromOldDB {
		idFactor = 100
	}

	varId := iotPayload.NodeId*idFactor + iotPayload.PropId

	rows, errr := stmt.Query(
		varId, iotPayload.Start,
		varId, iotPayload.Start, iotPayload.Stop,
		varId, iotPayload.Start,
		varId, iotPayload.Start, iotPayload.Stop)
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	var startTimestamp = 0
	var timestamp = 0
	var delta = 0
	var prevTimestamp = 0
	var cnt = 0

	respBuff.Write([]byte(fmt.Sprintf(`{"id":%d,"from":%d,"timedata":[`, iotPayload.VarId, iotPayload.Start)))
	// fmt.Printf(`{"id":%d,"from":%d\n`, req.Id, req.Start)

	for rows.Next() {
		err := rows.Scan(&timestamp, &delta)
		checkError(err)

		cnt++

		if cnt == 1 {
			startTimestamp = timestamp
		}

		var deltaTijd = timestamp - prevTimestamp

		if cnt > 1 {
			respBuff.Write([]byte(`,`))
		}
		respBuff.Write([]byte(fmt.Sprintf("%d,%d", deltaTijd, delta)))
		if cnt > 825 {
			// fmt.Printf("[%d,%d,%d]\n", cnt, prevTimestamp, timestamp)
			// fmt.Printf("[%d,%d,%d]\n", cnt, deltaTijd, deltaVal)
		}

		prevTimestamp = timestamp
	}

	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	respBuff.Write([]byte(fmt.Sprintf(`],"count":%d,"start":%d,"stop":%d,"stopdelta":%d}`, cnt, startTimestamp, timestamp, delta)))

	// fmt.Printf(`timedata: ],"count":%d,"start":%d,"stop":%d,"stopval":%d}\n\n`, cnt, startTimestamp, timestamp, delta)

	respBuff.Write([]byte("\n"))

	err = rows.Close()
	checkError(err)

	if errr := rows.Err(); errr != nil {
		checkError(errr)
	}

	err = stmt.Close()
	checkError(err)
}

func timedataDiff(respBuff *bytes.Buffer, iotPayload *iot.IotPayload) {

	var err error

	/*
	*  combine all values from domoLog and domoLogMem
	*  find 1 more values just over the edge.
	*  ! duplicates are automatically removed by the Distinct
	 */
	stmt, errr := database.Prepare(`
	Select Distinct * from
	(  	  ( SELECT timestamp, val from domoLog    WHERE id = ? and not val is null and timestamp <= ?                   ORDER BY timestamp DESC LIMIT 0,1 )
	Union ( SELECT timestamp, val from domoLog    WHERE id = ? and not val is null and timestamp > ? and timestamp <= ? ORDER BY timestamp )
	Union ( SELECT timestamp, val from domoLogMem WHERE id = ? and not val is null and timestamp <= ?                   ORDER BY timestamp DESC LIMIT 0,1 )
	Union ( SELECT timestamp, val from domoLogMem WHERE id = ? and not val is null and timestamp > ? and timestamp <= ? ORDER BY timestamp )
	) as result ORDER BY timestamp`)

	checkError(errr)

	defer stmt.Close()

	idFactor := iot.IDFactor

	if timeFromOldDB {
		idFactor = 100
	}

	varId := iotPayload.NodeId*idFactor + iotPayload.PropId

	rows, errr := stmt.Query(
		varId, iotPayload.Start,
		varId, iotPayload.Start, iotPayload.Stop,
		varId, iotPayload.Start,
		varId, iotPayload.Start, iotPayload.Stop)
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	var startTimestamp = 0
	var timestamp = 0
	var val = 0
	var prevTimestamp = 0
	var prevVal = 0
	var cnt = 0

	respBuff.Write([]byte(fmt.Sprintf(`{"id":%d,"from":%d,"timedata":[`, iotPayload.VarId, iotPayload.Start)))
	// fmt.Printf(`{"id":%d,"from":%d\n`, req.Id, req.Start)

	for rows.Next() {
		err := rows.Scan(&timestamp, &val)
		checkError(err)

		cnt++

		if cnt == 1 {
			startTimestamp = timestamp
		}

		var deltaVal = val - prevVal
		var deltaTijd = timestamp - prevTimestamp
		if deltaVal != 0 {

			if cnt > 1 {
				respBuff.Write([]byte(`,`))
			}
			respBuff.Write([]byte(fmt.Sprintf("%d,%d", deltaTijd, deltaVal)))
			if cnt > 825 {
				// fmt.Printf("[%d,%d,%d]\n", cnt, prevTimestamp, timestamp)
				// fmt.Printf("[%d,%d,%d]\n", cnt, deltaTijd, deltaVal)
			}

			prevTimestamp = timestamp
			prevVal = val
		}
	}

	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	respBuff.Write([]byte(fmt.Sprintf(`],"count":%d,"start":%d,"stop":%d,"stopval":%d}`, cnt, startTimestamp, timestamp, val)))

	// fmt.Printf(`timedata: ],"count":%d,"start":%d,"stop":%d,"stopval":%d}\n\n`, cnt, startTimestamp, timestamp, val)

	respBuff.Write([]byte("\n"))

	err = rows.Close()
	checkError(err)

	if errr := rows.Err(); errr != nil {
		checkError(errr)
	}

	err = stmt.Close()
	checkError(err)
}

func iotCommand(conn net.Conn, cmd string) (string, error) {

	//TODO check how 0x00 is handled

	var err error
	buff := make([]byte, 10)

	c := bufio.NewReader(conn)

	fmt.Printf("iotCmd cmd> %s\n", cmd)

	_, err = conn.Write([]byte(cmd + "\r"))

	if err != nil {

		return string(buff), err
	}

	bufff, _, errr := c.ReadLine()

	return string(bufff), errr

}

func connect(service string) (net.Conn, error) {

	conn, err := net.DialTimeout("tcp", service, 3*time.Second)

	if err == nil {
		// fmt.Printf("%s connected\r", service)

	} else {

		log.Fatal(fmt.Sprintf("idb: connect %s: %s", service, err.Error()))
	}

	return conn, err
}

func checkError(err error) {
	if err != nil {
		Error.Println(err)
	}
}

//------------------------------------
func startForwardService(port string, target net.Conn) error {

	forwardListener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return err
	}

	fmt.Printf("forwardListener :" + port + "\r")

	go serviceForwardListener(forwardListener, target)

	return err
}

func serviceForwardListener(listener net.Listener, target net.Conn) {

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		// multi threading:
		go handleForwardClient(conn, target)
	}
}

func handleForwardClient(conn net.Conn, target net.Conn) {

	fmt.Printf("handleForwardClient\r")

	go forward("fwdUp", conn, target)

	go forward("fwdDown", target, conn)
}

func forward(id string, source net.Conn, target net.Conn) {

	var char byte
	var err error

	defer source.Close() // close connection before exit

	c := bufio.NewReader(source)
	t := bufio.NewWriter(target)

	timeoutDuration := 3 * time.Second

	for {

		//source.SetReadDeadline(time.Now().Add(timeoutDuration))
		char, err = c.ReadByte()

		if err == nil {

			//fmt.Printf("%s < %x-%c\r", id, char, char )

			err = t.WriteByte(char)

			if err != nil {
				fmt.Printf("forward write err %s\n", err.Error())
				return
			}

			if char == 0x0d || char == 0x0a || c.Buffered() >= c.Size() {

				if c.Buffered() >= c.Size() {
					fmt.Printf("%s flush %d\n", id, t.Buffered())
				}

				target.SetWriteDeadline(time.Now().Add(timeoutDuration))
				t.Flush()

				// calc new deadline

			}

		} else {

			fmt.Printf("forward read err %s\n", err.Error())
			return
		}
	}
}

func init() {

	persistLogRunning = false
	dbCheckSum = "2019-11-02"
	timeFromOldDB = true

	dbJson = `[ 	
		{"Name":"nodesDef",
		"Columns":[ 
			{"Name":"nodeId" ,"ColType":"INT(10) NOT NULL"} 
			,{"Name":"name"  ,"ColType":"VARCHAR(100) DEFAULT NULL"}
			,{"Name":"connId","ColType":"INT(10) NOT NULL"} 
		],
		"PKey":"nodeId",
		"Engine":"InnoDB DEFAULT CHARSET=latin1"}

		,{"Name":"nodesDsk",
		"Columns":[ 
			{"Name":"nodeId"   ,"ColType":"INT(10) NOT NULL"} 
			,{"Name":"bootCount","ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"freeMem"  ,"ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"bootTime" ,"ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"} 
			,{"Name":"timestamp","ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"}
		], 
		"PKey":"nodeId", 
		"Engine":"InnoDB DEFAULT CHARSET=latin1"}

		,{"Name":"nodesMem",
		"Columns":[ 
			{"Name":"nodeId"   ,"ColType":"INT(10) NOT NULL"} 
			,{"Name":"bootCount","ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"freeMem"  ,"ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"bootTime" ,"ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"} 
			,{"Name":"timestamp","ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"}
		], 
		"PKey":"nodeId", 
		"Engine":"MEMORY DEFAULT CHARSET=latin1"}

		,{"Name":"propsDef",
		"Columns":[ 
			{"Name":"varId"    ,"ColType":"INT(10) NOT NULL"} 
			,{"Name":"nodeId"   ,"ColType":"INT(10) NOT NULL"} 
			,{"Name":"propId"   ,"ColType":"INT(10) NOT NULL"} 
			,{"Name":"name"     ,"ColType":"VARCHAR(100) DEFAULT NULL"} 

			,{"Name":"decimals" 	,"ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"logOption" 	,"ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"traceOption" 	,"ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"localMvc" 	,"ColType":"TINYINT(3) NOT NULL DEFAULT 0"} 
			,{"Name":"globalMvc" 	,"ColType":"TINYINT(3) NOT NULL DEFAULT 0"} 

			,{"Name":"drawType" 	,"ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"drawOffset" 	,"ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"drawColor"    ,"ColType":"VARCHAR(100) NOT NULL DEFAULT 'black'"} 
			,{"Name":"drawFactor" 	,"ColType":"DECIMAL(7,4) NOT NULL DEFAULT 0.0000"} 

			,{"Name":"refreshRate"  ,"ColType":"INT(10) NOT NULL DEFAULT 0"} 
		],
		"PKey":"varId", 
		"Engine":"InnoDB DEFAULT CHARSET=latin1"}

		,{"Name":"propsDsk",
		"Columns":[ 
			{"Name":"varId"     , "ColType":"INT(10) NOT NULL"} 
			,{"Name":"val"     	, "ColType":"BIGINT(20) DEFAULT NULL"} 
			,{"Name":"valString", "ColType":"VARCHAR(100) DEFAULT NULL"} 
			,{"Name":"valStamp" , "ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"} 
			,{"Name":"inErr" 	, "ColType":"TINYINT(3) NOT NULL DEFAULT 0"} 
			,{"Name":"err"   	, "ColType":"BIGINT(20) DEFAULT NULL"} 
			,{"Name":"errStamp" , "ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"} 
			,{"Name":"retryCnt"   ,"ColType":"INT(10) NOT NULL"} 
			,{"Name":"nextRefresh", "ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"} 
		],
		"PKey":"varId", 
		"Engine":"InnoDB DEFAULT CHARSET=latin1"}

		,{"Name":"propsMem",
		"Columns":[ 
			{"Name":"varId"     , "ColType":"INT(10) NOT NULL"} 
			,{"Name":"val"     	, "ColType":"BIGINT(20) DEFAULT NULL"} 
			,{"Name":"valString", "ColType":"VARCHAR(100) DEFAULT NULL"} 
			,{"Name":"valStamp" , "ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"} 
			,{"Name":"inErr" 	, "ColType":"TINYINT(3) NOT NULL DEFAULT 0"} 
			,{"Name":"err"   	, "ColType":"BIGINT(20) DEFAULT NULL"} 
			,{"Name":"errStamp" , "ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"} 
			,{"Name":"retryCnt"   ,"ColType":"INT(10) NOT NULL"} 
			,{"Name":"nextRefresh", "ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"} 
		],
		"PKey":"varId", 
		"Engine":"MEMORY DEFAULT CHARSET=latin1"}

		,{"Name":"logMem",
		"Columns":[ 
			{"Name":"varId"  , "ColType":"INT(10) NOT NULL"} 
			,{"Name":"stamp" , "ColType":"BIGINT(20) UNSIGNED NOT NULL"} 
			,{"Name":"val"   , "ColType":"BIGINT(20) DEFAULT NULL"} 
			,{"Name":"delta" , "ColType":"BIGINT(20) DEFAULT NULL"} 
		],
		"PKey":"varId, stamp", 
		"Engine":"MEMORY DEFAULT CHARSET=latin1"}

		,{"Name":"logDsk",
		"Columns":[ 
			{"Name":"varId"  , "ColType":"INT(10) NOT NULL"} 
			,{"Name":"stamp" , "ColType":"BIGINT(20) UNSIGNED NOT NULL"} 
			,{"Name":"val"   , "ColType":"BIGINT(20) DEFAULT NULL"} 
			,{"Name":"delta" , "ColType":"BIGINT(20) DEFAULT NULL"} 
		],
		"PKey":"varId, stamp", 
		"Engine":"InnoDB DEFAULT CHARSET=latin1"}

		,{"Name":"graphPlans",
		"Columns":[ 
			{"Name":"name"     ,"ColType":"VARCHAR(255) NOT NULL"} 
			,{"Name":"id"       ,"ColType":"INT(10) NOT NULL"} 
		],
		"PKey":"id,name, stamp", 
		"Engine":"InnoDB DEFAULT CHARSET=latin1"}

	]
	`
}

func InitLogging(
	traceHandle io.Writer,
	infoHandle io.Writer,
	warningHandle io.Writer,
	errorHandle io.Writer) {

	Detail = log.New(traceHandle,
		"",
		0)

	Trace = log.New(traceHandle,
		"",
		log.Ltime)

	Info = log.New(infoHandle,
		"",
		log.Ltime)

	Warning = log.New(warningHandle,
		"w)",
		log.Ldate|log.Ltime|log.Lshortfile)

	Error = log.New(errorHandle,
		"e)",
		log.Ldate|log.Ltime|log.Lshortfile)

}
