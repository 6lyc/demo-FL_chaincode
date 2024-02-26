package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/protos/peer"


)

var mutex = &sync.Mutex{}

type SmartContract struct {
}

func int64ToBytes(num int64) []byte {
	s := strconv.FormatInt(num, 10)
	return []byte(s)
}

func bytesToint64(bytes []byte) int64 {
	i, _ := strconv.ParseInt(string(bytes), 10, 64)
	return i
}

//此处进行初始化工作（客户端数目：0）

func (s *SmartContract) Init(stub shim.ChaincodeStubInterface) peer.Response {
	var clients_count = int64(0)
	buf := int64ToBytes(clients_count)
	err := stub.PutState("clients_count", buf)
	if err != nil {
		// return fmt.Errorf("Failed to put to world state. %s", )
		return shim.Error(err.Error())
	}
	return shim.Success(nil)
}

func (s *SmartContract) Invoke(stub shim.ChaincodeStubInterface) peer.Response {
	fn, args := stub.GetFunctionAndParameters()
	if fn == "regist" {
		return s.regist(stub, args)
	} else if fn == "upload_model" {
		return s.upload_model(stub, args)
	} else if fn == "download_model" {
		return s.download_model(stub, args)
	}
	return shim.Error("Invalid invoke function name. Expecting \"regist\" \"upload_model\" \"download_model\"")
}

/**
 * 注册客户端
 */
func (s *SmartContract) regist(stub shim.ChaincodeStubInterface, args []string) peer.Response {
	if len(args) < 1 {
		// return fmt.Errorf("Incorrect arguments. Expecting client_id")
		return shim.Error("Incorrect arguments. Expecting client_id")
	}
	client_id := args[0]
	key := "cid_" + client_id
	registed, err := stub.GetState(key)
	if err != nil {
		return shim.Error("Failed to get state")
	}
	if registed == nil {
		registed = []byte("1")
		err := stub.PutState(key, []byte(registed))
		if err != nil {
			return shim.Error("Failed to save client")
		}
		clients_count, err := stub.GetState("clients_count")
		if err != nil {
			return shim.Error("Failed to get client counts")
		}
		if clients_count == nil {
			return shim.Error("client counts is nil")
		}
		clients_count_int := bytesToint64(clients_count)
		clients_count_int += 1
		buf := int64ToBytes(clients_count_int)
		err = stub.PutState("clients_count", buf)
		if err != nil {
			return shim.Error("Failed to put clients count")
		}
	} else {
		return shim.Success([]byte("Already Registed!"))
	}
	return shim.Success([]byte("Regist Successfully"))
}

func (s *SmartContract) upload_model(stub shim.ChaincodeStubInterface, args []string) peer.Response {
	/*arg0: cid string
	arg1: epoch string(int-->string)
	arg2: model_para string
	arg3: train_method fedavg fednova scanffold
	In program  the arg2 will be tranfer to []byte and then to dict by json.unmarshall
	if len(args) < 3 {
		// return fmt.Errorf("Incorrect arguments. Expecting cid, epoch and model")
		return shim.Error("Incorrect arguments. Expecting cid, epoch and model")
	}*/
	mutex.Lock()
	defer mutex.Unlock()
	if len(args) < 3 {
		// return fmt.Errorf("Incorrect arguments. Expecting cid, epoch and model")
		return shim.Error("Incorrect arguments. Expecting cid, epoch and model")
	}
	cid := args[0]
	epoch := args[1]
	train_method := args[3]
	var model map[string]interface{}
	model = make(map[string]interface{})
	err := json.Unmarshal([]byte(args[2]), &model) //将字符串转化成序列
	if err != nil {
		// return fmt.Errorf("Failed to unmarshal model with error: %s", err)
		return shim.Error("Failed to unmarshal passed-in model")
	}

	//从链上查询出本轮所有的模型（为了后续的更新）
	key := "epoch_" + epoch
	//Attention：这里是不是key还没有的时候没有办法查询以及序列化。
	//Attention：考虑将空字典先初始化进这个key里面
	value, err := stub.GetState(key)
	if err != nil {
		// return fmt.Errorf("Failed to query models with error: %s", err)
		return shim.Error("Failed to query models")
	}

	var models_epoch map[string]map[string]interface{}
	models_epoch = make(map[string]map[string]interface{})

	if value != nil {
		err = json.Unmarshal(value, &models_epoch)
		if err != nil {
			// return fmt.Errorf("Failed to unmarshal models with error: %s", err)
			return shim.Error("Failed to unmarshall on-chain models")
		}
	}
	_, ok := models_epoch[cid]
	if ok {
		// return fmt.Errorf("uploaded model")
		return shim.Error("uploaded model")
	}
	//未上传过的模型，先放入字典中
	models_epoch[cid] = model

	//计算已经上传模型个数
	models_count := int64(len(models_epoch))

	//新模型列表保存到链上
	new_json_bytes, err := json.Marshal(models_epoch)
	if err != nil {
		// return fmt.Errorf("Failed to marshal new models with error: %s", err)
		return shim.Error("Failed to marshal new models")
	}
	err = stub.PutState(key, new_json_bytes)
	if err != nil {
		// return fmt.Errorf("Failed to save new models with error: %s", err)
		return shim.Error("Failed to save new models")
	}

	// fednova
	if train_method == "fednova" {
		var a_i float64
		var norm_grad map[string]interface{}
		var n_i float64
		err_1 := json.Unmarshal([]byte(args[4]), &a_i)       //将字符串转化成序列
		err_2 := json.Unmarshal([]byte(args[5]), &norm_grad) //将字符串转化成序列
		err_3 := json.Unmarshal([]byte(args[6]), &n_i)       //将字符串转化成序列
		if err_1 != nil || err_2 != nil || err_3 != nil {
			// return fmt.Errorf("Failed to unmarshal model with error: %s", err)
			return shim.Error("Failed to unmarshal model")
		}

		//从链上查询出本轮所有的参数（为了后续的更新）
		key := "a_i_epoch_" + epoch
		value, err := stub.GetState(key)
		if err != nil {
			// return fmt.Errorf("Failed to query models with error: %s", err)
			return shim.Error("Failed to query models")
		}
		var a map[string]float64
		err = json.Unmarshal(value, &a)
		if err != nil {
			// return fmt.Errorf("Failed to unmarshal models with error: %s", err)
			return shim.Error("Failed to unmarshal models")
		}
		_, ok := a[cid]
		if ok {
			// return fmt.Errorf("uploaded model")
			return shim.Error("uploaded model")
		}
		//未上传过的模型，先放入字典中
		a[cid] = a_i

		//新模型列表保存到链上
		new_json_bytes, err := json.Marshal(a)
		if err != nil {
			// return fmt.Errorf("Failed to marshal new models with error: %s", err)
			return shim.Error("Failed to marshal new models")
		}
		err = stub.PutState(key, new_json_bytes)
		if err != nil {
			// return fmt.Errorf("Failed to save new models with error: %s", err)
			return shim.Error("Failed to save new models")
		}

		//从链上查询出本轮所有的参数（为了后续的更新）
		key = "n_i_epoch_" + epoch
		value, err = stub.GetState(key)
		if err != nil {
			// return fmt.Errorf("Failed to query models with error: %s", err)
			return shim.Error("Failed to query models")
		}
		var n map[string]float64
		err = json.Unmarshal(value, &n)
		if err != nil {
			// return fmt.Errorf("Failed to unmarshal models with error: %s", err)
			return shim.Error("Failed to unmarshal models")
		}
		_, ok = n[cid]
		if ok {
			// return fmt.Errorf("uploaded model")
			return shim.Error("uploaded model")
		}
		//未上传过的模型，先放入字典中
		n[cid] = n_i

		//新模型列表保存到链上
		new_json_bytes, err = json.Marshal(n)
		if err != nil {
			// return fmt.Errorf("Failed to marshal new models with error: %s", err)
			return shim.Error("Failed to marshal new models")
		}
		err = stub.PutState(key, new_json_bytes)
		if err != nil {
			// return fmt.Errorf("Failed to save new models with error: %s", err)
			return shim.Error("Failed to save new models")
		}

		//从链上查询出本轮所有的模型（为了后续的更新）
		key = "norm_grad_epoch_" + epoch
		value, err = stub.GetState(key)
		if err != nil {
			// return fmt.Errorf("Failed to query models with error: %s", err)
			return shim.Error("Failed to query models")
		}
		var d_list map[string]map[string]interface{}
		err = json.Unmarshal(value, &d_list)
		if err != nil {
			// return fmt.Errorf("Failed to unmarshal models with error: %s", err)
			return shim.Error("Failed to unmarshal models")
		}
		_, ok = d_list[cid]
		if ok {
			// return fmt.Errorf("uploaded model")
			return shim.Error("Uploaded model")
		}
		//未上传过的模型，先放入字典中
		d_list[cid] = norm_grad

		//新模型列表保存到链上
		new_json_bytes, err = json.Marshal(d_list)
		if err != nil {
			// return fmt.Errorf("Failed to marshal new models with error: %s", err)
			return shim.Error("Failed to marshal new models")
		}
		err = stub.PutState(key, new_json_bytes)
		if err != nil {
			// return fmt.Errorf("Failed to save new models with error: %s", err)
			return shim.Error("Failed to save new models")
		}

		//计算已经上传模型个数
		models_count := int64(len(models_epoch))

		//查询在线客户端数量
		client_counts_key := "clients_count"
		counts_bytes, err := stub.GetState(client_counts_key)
		if err != nil {
			// return fmt.Errorf("Failed to get clients count with error: %s", err)
			return shim.Error("Failed to get clients count")
		}
		var counts int64
		counts, err = strconv.ParseInt(string(counts_bytes), 10, 64)
		if err != nil {
			// return fmt.Errorf("Failed to convert bytes to int with error: %s", err)
			return shim.Error("Failed to convert bytes to int")
		}
		if counts == models_count {
			//上传模型数量等于客户端数量，即所有客户端均上传了模型，需要进行聚合操作
			aggregated_model, err := aggregate_fednova(d_list, a, n)
			//保存到链上
			global_model_key := "global_" + epoch
			model_bytes, err := json.Marshal(aggregated_model)
			if err != nil {
				// return fmt.Errorf("failed to marshal aggregated model with error: %s", err)
				return shim.Error("Failed to marshal aggregated model")
			}
			err = stub.PutState(global_model_key, model_bytes)
			if err != nil {
				// return fmt.Errorf("failed to put aggregated model with error: %s", err)
				return shim.Error("Failed to put aggregated model")
			}
		}
		return shim.Success([]byte("OK"))
	}

	// scaffold
	if train_method == "scaffold" {
		var c_delta_para map[string]interface{}
		err_4 := json.Unmarshal([]byte(args[7]), &c_delta_para) //将字符串转化成序列
		if err_4 != nil {
			// return fmt.Errorf("Failed to unmarshal model with error: %s", err)
			return shim.Error("Failed to unmarshal model")
		}

		//从链上查询出本轮所有的模型（为了后续的更新）
		key = "cList_epoch_" + epoch
		value, err = stub.GetState(key)
		if err != nil {
			// return fmt.Errorf("Failed to query models with error: %s", err)
			return shim.Error("Failed to query models")
		}
		var c_list map[string]map[string]interface{}
		err = json.Unmarshal(value, &c_list)
		if err != nil {
			// return fmt.Errorf("Failed to unmarshal models with error: %s", err)
			return shim.Error("Failed to unmarshal models")
		}
		_, ok = c_list[cid]
		if ok {
			// return fmt.Errorf("uploaded model")
			return shim.Error("uploaded model")
		}
		//未上传过的模型，先放入字典中
		c_list[cid] = c_delta_para

		//新模型列表保存到链上
		new_json_bytes, err = json.Marshal(c_list)
		if err != nil {
			// return fmt.Errorf("Failed to marshal new models with error: %s", err)
			return shim.Error("Failed to marshal new models")
		}
		err = stub.PutState(key, new_json_bytes)
		if err != nil {
			// return fmt.Errorf("Failed to save new models with error: %s", err)
			return shim.Error("Failed to save new models")
		}

		//计算已经上传模型个数
		models_count := int64(len(models_epoch))

		//查询在线客户端数量
		client_counts_key := "clients_count"
		counts_bytes, err := stub.GetState(client_counts_key)
		if err != nil {
			// return fmt.Errorf("Failed to get clients count with error: %s", err)
			return shim.Error("Failed to get clients count")
		}
		var counts int64
		counts, err = strconv.ParseInt(string(counts_bytes), 10, 64)
		if err != nil {
			// return fmt.Errorf("Failed to convert bytes to int with error: %s", err)
			return shim.Error("Failed to convert bytes to int")
		}
		if counts == models_count {
			//上传模型数量等于客户端数量，即所有客户端均上传了模型，需要进行聚合操作
			aggregated_model, c_global_model, err := aggregate_scaffold(models_epoch, c_list)
			//保存到链上
			global_model_key := "global_" + epoch
			model_bytes, err := json.Marshal(aggregated_model)
			if err != nil {
				// return fmt.Errorf("failed to marshal aggregated model with error: %s", err)
				return shim.Error("Failed to marshal aggregated model")
			}
			err = stub.PutState(global_model_key, model_bytes)
			if err != nil {
				// return fmt.Errorf("failed to put aggregated model with error: %s", err)
				return shim.Error("Failed to put aggregated model")
			}

			c_global_model_key := "c_global_" + epoch
			c_model_bytes, err := json.Marshal(c_global_model)
			if err != nil {
				// return fmt.Errorf("failed to marshal aggregated model with error: %s", err)
				return shim.Error("Failed to marshal aggregated model")
			}
			err = stub.PutState(c_global_model_key, c_model_bytes)
			if err != nil {
				// return fmt.Errorf("failed to put aggregated model with error: %s", err)
				return shim.Error("Failed to put aggregated model")
			}
		}
		return shim.Success([]byte("OK"))
	}

	//查询在线客户端数量
	client_counts_key := "clients_count"
	counts_bytes, err := stub.GetState(client_counts_key)
	if err != nil {
		// return fmt.Errorf("Failed to get clients count with error: %s", err)
		return shim.Error("Failed to get clients count")
	}
	var counts int64
	counts = bytesToint64(counts_bytes)
	counts, err = strconv.ParseInt(string(counts_bytes), 10, 64)
	if err != nil {
		// return fmt.Errorf("Failed to convert bytes to int with error: %s", err)
		fmt.Println("Failed to convert bytes to int with error: %s", err)
		return shim.Error("Failed to convert bytes")
	}
	//return shim.Error(string(counts) + " " + string(models_count))
	//if counts != models_count {
	//	return shim.Success([]byte("bad OK"))
	//}
	//return shim.Success([]byte(strconv.FormatInt(models_count, 10)))
	if counts == models_count {
		aggregated_model, _ := aggregate(models_epoch, counts)
		if aggregated_model == nil {
			return shim.Error("Failed to aggregate model")
		}
		//保存到链上
		global_model_key := "global_" + epoch
		model_bytes, _ := json.Marshal(aggregated_model)
		if err != nil {
			// return fmt.Errorf("failed to marshal aggregated model with error: %s", err)
			return shim.Error("Failed to marshal aggregated model")
		}
		err = stub.PutState(global_model_key, model_bytes)
		if err != nil {
			// return fmt.Errorf("failed to put aggregated model with error: %s", err)
			return shim.Error("Failed to put aggregated model")
		}
		return shim.Success([]byte("Aggregated!"))
	}
	return shim.Success([]byte("Upload but not aggregated!"))
}

/**
 * 下载模型
 */

type MyResponse struct {
	String1 string
	String2 string
}

func (s *SmartContract) download_model(stub shim.ChaincodeStubInterface, args []string) peer.Response {
	if len(args) < 4 {
		// return "", fmt.Errorf("Incorrect number of arguments required for cid and epoch")
		return shim.Error("Incorrect number of arguments required for cid and epoch")
	}
	key := "global_" + args[1]
	train_method := args[3]
	if train_method == "scaffold" {
		global_model_key := "global_" + args[1]
		c_global_model_key := "c_global_" + args[1]
		value_1, err_1 := stub.GetState(global_model_key)
		value_2, err_2 := stub.GetState(c_global_model_key)
		if err_1 != nil {
			// return "", fmt.Errorf("Failed to get global model of scaffold with error: %s", err_1)
			return shim.Error("Failed to get global model of scaffold")
		}
		if value_1 == nil {
			// return "", fmt.Errorf("Cannot read world state pair with %s. Does not exist", global_model_key)
			return shim.Error("Cannot read world state pair")
		}
		if err_2 != nil {
			// return "", fmt.Errorf("Failed to get global model of scaffold with error: %s", err_2)
			return shim.Error("Failed to get global model of scaffold")
		}
		if value_2 == nil {
			// return "", fmt.Errorf("Cannot read world state pair with %s. Does not exist", c_global_model_key)
			return shim.Error("Cannot read world state pair")
		}

		response := MyResponse{
			String1: string(value_1),
			String2: string(value_2),
		}
		payload, err := json.Marshal(response)
		if err != nil {
			return shim.Error("error marshaling response")
		}
		return shim.Success([]byte(string(payload)))
	}
	// err := stub.PutState(key, []byte("66666"))
	//if err != nil {
	//	// return "", fmt.Errorf("Failed to get global model with error: %s", err)
	//	return shim.Error("Failed to put 6666666")
	//
	value, err := stub.GetState(key)
	if err != nil {
		// return "", fmt.Errorf("Failed to get global model with error: %s", err)
		return shim.Error("Failed to get global model")
	}
	if value == nil {
		// return "", fmt.Errorf("Cannot read world state pair with %s. Does not exist", key)
		return shim.Error("Cannot read world state pair!!")
	}
	return shim.Success([]byte(string(value)))
}

/**
 * 进行模型聚合
 */
// map 类型是无序的，与回传的orderdict矛盾(map按默认顺序不变)
func aggregate(models_dict map[string]map[string]interface{}, models_count int64) (map[string]interface{}, error) {
	var global_model map[string]interface{}
	for k, v := range models_dict {
		if global_model == nil {
			global_model = models_dict[k]
		} else {
			for m_k, _ := range global_model {
				switch g := global_model[m_k].(type) {
				case []float64:
					v_v := v[m_k].([]float64)
					global_model[m_k] = vector_add(g, v_v)
				case [][]float64:
					v_v := v[m_k].([][]float64)
					global_model[m_k] = matrix_add(g, v_v)
				default:
					fmt.Printf("The parameter type %T is invalid!\n", v)
				}
			}
		}
	}

	for k, v := range global_model {
		switch g := v.(type) {
		case []float64:
			global_model[k] = vector_div(g, float64(models_count))
		case [][]float64:
			global_model[k] = matrix_div(g, float64(models_count))
		default:
			fmt.Printf("The parameter type %T is invalid!\n", v)
		}
	}

	return global_model, nil
}

func vector_add(vector1 []float64, vector2 []float64) []float64 {
	ret := make([]float64, len(vector1))
	for index, value := range vector1 {
		ret[index] = value + vector2[index]
	}
	return ret
}
func matrix_add(mat1 [][]float64, mat2 [][]float64) [][]float64 {
	ret := make([][]float64, len(mat1))
	for index, value := range mat1 {
		ret[index] = make([]float64, len(value))
		for index_i, value_i := range mat1[index] {
			ret[index][index_i] = value_i + mat2[index][index_i]
		}
	}
	return ret
}

func vector_div(vector []float64, num float64) []float64 {
	ret := make([]float64, len(vector))
	for index, value := range vector {
		ret[index] = value / num
	}
	return ret
}

func matrix_div(matrix [][]float64, num float64) [][]float64 {
	ret := make([][]float64, len(matrix))
	for index, value := range matrix {
		ret[index] = make([]float64, len(value))
		for index_i, value_i := range value {
			ret[index][index_i] = value_i / num
		}
	}
	return ret
}

func aggregate_scaffold(models_dict map[string]map[string]interface{}, cList map[string]map[string]interface{}) (map[string]interface{}, map[string]interface{}, error) {
	var global_model map[string]interface{}

	// 初始化全局模型
	for k, _ := range models_dict {
		if global_model == nil {
			global_model = models_dict[k]
			break
		}
	}

	total_delta := make(map[string]interface{})
	for _, values := range cList {
		for key, value := range values {
			if _, ok := total_delta[key]; !ok {
				// 初始化结果 map 中的键值对
				total_delta[key] = value
			} else {
				// 累加当前键值对的值
				switch value.(type) {
				case []float64:
					total_delta[key] = vector_add(total_delta[key].([]float64), value.([]float64))
				case [][]float64:
					total_delta[key] = matrix_add(total_delta[key].([][]float64), value.([][]float64))
				default:
					fmt.Printf("The parameter type is invalid!\n")
				}
			}
		}
	}

	// 对结果进行平均
	for key := range total_delta {
		total_delta[key] = total_delta[key].(float64) / float64(len(cList))
	}

	c_global_para := global_model
	for key := range c_global_para {
		// 累加当前键值对的值
		switch c_global_para[key].(type) {
		case []float64:
			total_delta[key] = vector_add(c_global_para[key].([]float64), total_delta[key].([]float64))
		case [][]float64:
			total_delta[key] = matrix_add(c_global_para[key].([][]float64), total_delta[key].([][]float64))
		default:
			fmt.Printf("The parameter type is invalid!\n")
		}
	}

	// total_data_points := 3000
	// fed_avg_freqs := [3]int{1000 / 3000, 1000 / 3000, 1000 / 3000}
	var global_model_t map[string]interface{}
	for _, values := range models_dict {
		if global_model_t == nil {
			global_model_t = values
		} else {
			for key, value := range values {
				global_model_t[key] = global_model_t[key].(float64) + value.(float64)*(1/3)
				switch value.(type) {
				case []float64:
					global_model_t[key] = vector_add(global_model_t[key].([]float64), vector_div(value.([]float64), 3))
				case [][]float64:
					global_model_t[key] = matrix_add(global_model_t[key].([][]float64), matrix_div(value.([][]float64), 3))
				default:
					fmt.Printf("The parameter type is invalid!\n")
				}

			}
		}
	}

	return global_model_t, c_global_para, nil
}

func aggregate_fednova(d_list map[string]map[string]interface{}, a_list map[string]float64, n_list map[string]float64) (map[string]interface{}, error) {
	total_n := 0.0

	var global_model map[string]interface{}
	// 初始化全局模型
	for k, _ := range d_list {
		if global_model == nil {
			global_model = d_list[k]
			break
		}
	}

	for _, n := range n_list {
		total_n += n
	}

	d_total_round := make(map[string]interface{})
	for i, d_para := range d_list {
		for key, value := range d_para {
			switch value.(type) {
			case []float64:
				d_total_round[key] = vector_add(d_total_round[key].([]float64), vector_div(value.([]float64), total_n/n_list[i]))
			case [][]float64:
				d_total_round[key] = matrix_add(d_total_round[key].([][]float64), matrix_div(value.([][]float64), total_n/n_list[i]))
			default:
				fmt.Printf("The parameter type is invalid!\n")
			}
		}
	}

	coeff := 0.0
	for i, a := range a_list {
		coeff += a * n_list[i] / total_n
	}

	updated_model := make(map[string]interface{})
	for key, value := range global_model {
		updated_model[key] = value
		switch value.(type) {
		case []float64:
			updated_model[key] = vector_add(value.([]float64), vector_div(d_total_round[key].([]float64), -1/coeff))
		case [][]float64:
			updated_model[key] = matrix_add(value.([][]float64), matrix_div(d_total_round[key].([][]float64), -1/coeff))
		default:
			fmt.Printf("The parameter type is invalid!\n")
		}
	}
	return updated_model, nil
}

func main() {
	err := shim.Start(new(SmartContract))
	if err != nil {
		fmt.Printf("Error starting Simple chaincode: %s", err)
	}
}
