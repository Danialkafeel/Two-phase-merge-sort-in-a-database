#include <bits/stdc++.h>
#include <fstream>
#include <thread>
#define ll long long
using namespace std; 

class MinHeapElement{
public:
	int file_no;
	vector<string> tuple;
	MinHeapElement(int file,vector<string> v){
		file_no = file;
		tuple = v;
	}
};
vector<int> index_sort_cols;
bool asc;
class MinHeapCompare{
public:
	bool operator()(MinHeapElement p1, MinHeapElement p2){
		for(int col:index_sort_cols){
			if(p1.tuple[col] != p2.tuple[col]){
				if(asc)
					return p1.tuple[col] < p2.tuple[col];
				else
					return p1.tuple[col] > p2.tuple[col];
			}
		}
		return true;
	}
};
class Two_phase_MS
{
public:
	string inputFile,outputFile;
	vector<string> columns;
	unordered_map<string, ll> colSize;			//	col_name -> size map
	ll chunk_size, tuple_size, total_records;
	int num_threads;
	double main_mem_size;
	ll mem_per_thread;
	ifstream finn;
	string delimiter = "  ";

	Two_phase_MS(string f1, string f2, string main_mem_size, string num_threads){
		inputFile = f1;
		outputFile = f2;
		this->main_mem_size = stod(main_mem_size)*1000000;
		this->num_threads = stoi(num_threads);
		mem_per_thread = this->main_mem_size/this->num_threads;
		finn.open(inputFile);
		set_tupleSize_and_cols();
		chunk_size = (this->mem_per_thread)/tuple_size;
		cout<<"chunk_size = "<<chunk_size<<endl;
		cout<<"tuple_size = "<<tuple_size<<endl;
		cout<<"main_mem_size = "<<this->main_mem_size<<endl;
		total_records = get_lines_count();
		cout<<"total_records = "<<total_records<<endl;
		cout<<"-------------------------------------------------\n\n";
	}
	ll get_lines_count(){
		ll num = 0;
		string line;
		ifstream myfile(inputFile);
		while (std::getline(myfile, line))
			++num;
		return num;
	}
	void set_tupleSize_and_cols(){
		tuple_size = 0;
		ifstream fin;
		fin.open("metadata.txt");
		string line;
		while(fin){
			getline(fin,line);
			if(line == "")
				break;
			size_t pos = line.find(',');
			string col_name = strip(line.substr(0,pos));
			line.erase(0,pos+1);
			string col_size = strip(line);
			columns.push_back(col_name);
			colSize[col_name] = stoll(col_size);
		}
		for(string s:columns){
			// cout<<s<<" : "<<colSize[s]<<endl;
			tuple_size += colSize[s]+2;
		}
		tuple_size -= 2;
		fin.close();
	}
	string strip(string str){
		int left = 0;
		while(str[left++] == ' ');
		str.erase(0,left-1);
		int right = str.size()-1;
		while(str[right--] == ' ');
		str.erase(right+2,str.size()-1);
		return str;
	}
	~Two_phase_MS(){
		// fclose(fp);
		finn.close();
	}
	vector<string> parse_row(string line){
		vector<string> ans;
		for(string col:columns){
			ans.push_back(line.substr(0,colSize[col]));
			line.erase(0,colSize[col]);
			if(line != "")
				line.erase(0,2);
		}
		return ans;
	}
	class comparator
	{
		vector<int> sort_cols;
		bool asc;
	public:
		comparator(bool asc, vector<int> v){
			sort_cols = v;
			this->asc = asc;
		}
		bool operator()(vector<string> v1,vector<string> v2){
			for(int col:this->sort_cols){
				if(v1[col] != v2[col]){
					if(this->asc)
						return v1[col] < v2[col];
					else
						return v1[col] > v2[col];
				}
			}
			return true;
		}
	};
	vector<int> name2index(vector<string> sort_cols){
		vector<int> ans;
		for(string col:sort_cols){
			bool found = false;
			for(int i=0;i<columns.size();i++){
				if(columns[i] == col){
					ans.push_back(i);
					found = true;
					break;
				}
			}
			if(not found){
				cout<<col<<" column not present in the table\n";
				ans.clear();
				return ans;
			}
		}
		return ans;
	}
	string convert_to_string(vector<string> v){
		string ans = "";
		for(int i=0;i<v.size()-1;i++){
			string s = v[i];
			ans += s + delimiter;
		}
		ans += v.back();
		return ans;
	}
	void clear_vectorOfvectors(vector<vector<string>> &v){
		for(auto vv:v)
			vv.clear();
		v.clear();
	}
	void sort_temp_and_store(int chunk_no, vector<vector<string>> tuples_to_sort, bool asc, vector<int> index_sort_cols){
		comparator cmp(asc,index_sort_cols);
		cout<<"Sorting #"<<chunk_no+1<<" sublist\n";
		sort(tuples_to_sort.begin(),tuples_to_sort.end(),cmp);
		ofstream outfile;
		string temp = "danny_temp_"+to_string(chunk_no);
		outfile.open(temp.c_str());
		cout<<"Writing to disk #"<<chunk_no+1<<"\n\n";
		for(int j=0;j<chunk_size;j++){
			string output = convert_to_string(tuples_to_sort[j])+"\n";
			outfile.write(output.c_str(),output.size());
		}
		outfile.close();
	}
	void Sort(bool ascd, vector<string> sort_cols){
		if(!finn.is_open()){
			cout<<"Input File doesn't exist!\nAborting..";
			return;
		}
		ll temp = mem_per_thread/tuple_size;
		ll no_of_files = ceil(1.0*total_records/temp);
		// cout<<"no_of_files = "<<no_of_files<<endl;
		if(chunk_size == 0 or (tuple_size*no_of_files > main_mem_size)) {
			cout<<"Two-way merge sort not possible with given memory constraints\n";
			return;
		}
		index_sort_cols = name2index(sort_cols);
		asc = ascd;
		if(index_sort_cols.size() == 0){
			return;
		}
		comparator cmp(asc,index_sort_cols);
		int i=0, chunk_no = 0;
		vector<vector<string>> tuples_to_sort;
		for(auto v:tuples_to_sort){
			v.reserve(columns.size());
		}
		cout<<"## Running phase 1\n";
		thread mythreads[num_threads];
		while(finn){
			// cout<<"i = "<<i<<"\n";
			i++;
			string line;
			getline(finn,line);
			if(line == "")
				break;
			tuples_to_sort.push_back(parse_row(line));
			if(i == chunk_size){
				if(chunk_no < num_threads){
					mythreads[chunk_no] = thread(&Two_phase_MS::sort_temp_and_store,this,chunk_no,tuples_to_sort,asc,index_sort_cols);
				}
				else{
					mythreads[chunk_no%num_threads].join();
					mythreads[chunk_no%num_threads] = thread(&Two_phase_MS::sort_temp_and_store,this,chunk_no,tuples_to_sort,asc,index_sort_cols);
				}
				chunk_no++;
				i = 0;
				clear_vectorOfvectors(tuples_to_sort);
			}
		}
		for(int j=0;j<num_threads;j++){
			if(mythreads[j].joinable())
				mythreads[j].join();
		}
		// cout<<"Yo\n";
		if(i > 1){		// last chunk
			// cout<<i-1<<" tuples in last chunk\n";
			cout<<"Sorting #"<<chunk_no+1<<" sublist\n";
			sort(tuples_to_sort.begin(),tuples_to_sort.end(),cmp);
			ofstream outfile;
			string temp = "danny_temp_"+to_string(chunk_no);
			outfile.open(temp.c_str());
			cout<<"Writing to disk #"<<chunk_no+1<<"\n";
			for(int j=0;j<i-1;j++){															// i or i-1
				string output = convert_to_string(tuples_to_sort[j])+"\n";
				outfile.write(output.c_str(),output.size());
			}
			outfile.close();
			chunk_no++;
		}
		if(chunk_no*tuple_size > main_mem_size){
			cout<<"\033[3mError !\n\033[0m";
			// delete_temp_files(chunk_no,false);
			// return;
		}
		merge_files(chunk_no);
		delete_temp_files(chunk_no,true);
		cout<<"### Completed execution\n";
	}
	
	void merge_files(int no_of_files){
		cout<<"\n## Running phase 2\n";
		cout<<"No of temp files created => "<<no_of_files<<endl;
		ofstream outfile;
		outfile.open(outputFile.c_str());
		priority_queue<MinHeapElement,vector<MinHeapElement>,MinHeapCompare> myheap;
		// comparator cmp(asc,index_sort_cols);
		// list<pair<int,vector<string>>> myheap;
		ifstream t_fp[no_of_files];
		// cout<<"Sorting...\n";
		for(int i=0;i<no_of_files;i++){
			t_fp[i].open(("danny_temp_"+to_string(i)).c_str());
			string line;
			getline(t_fp[i],line);
			MinHeapElement temp_elem(i,parse_row(line));
			myheap.push(temp_elem);
		}
		// make_heap(myheap.begin(),myheap.end(),cmp);
		cout<<"Writing to disk...\n";
		while(!myheap.empty()){
			MinHeapElement mini = myheap.top();
			myheap.pop();
			string line;
			getline(t_fp[mini.file_no],line);
			if(t_fp[mini.file_no] and line != ""){
				MinHeapElement temp_elem(mini.file_no,parse_row(line));
				myheap.push(temp_elem);
			}
			else{
				cout<<mini.file_no<<"th temp file completed !\n";
			}
			string temp = convert_to_string(mini.tuple)+"\n";
			outfile.write(temp.c_str(),temp.size());
		}
		outfile.close();
		// cout<<"Done!\n";
	}
	void delete_temp_files(int no_of_files,bool toPrint){
		for(int i=0;i<no_of_files;i++){
			string f = "danny_temp_"+to_string(i);
			remove(f.c_str());
		}
		if(toPrint)
			cout<<"Temp files deleted\n";
	}
};

  
int main(int argc, char* argv[]) 
{ 
	if(argc < 7){
		cout<<"Invalid Arguments\n";
		return 1;
	}
	string inputFile, outputFile, main_mem_size, asc, num_threads;
	inputFile = argv[1];
	outputFile = argv[2];
	main_mem_size = argv[3];
	num_threads = argv[4];
	asc = argv[5];

	vector<string> sort_cols;
	for(int i = 6;i < argc; i++){
		sort_cols.push_back(argv[i]);
	}

	Two_phase_MS obj(inputFile, outputFile, main_mem_size, num_threads);
	if(asc != "asc" and asc != "desc"){
		cout<<"Either provide ascending or descending order\n";
		return 1;
	}

  	obj.Sort(asc == "asc", sort_cols);

    return 0; 
}