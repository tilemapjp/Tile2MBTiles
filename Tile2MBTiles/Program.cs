using System;
using System.IO;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Linq;
using Mono.Data.Sqlite;
using System.Security.Cryptography;
using System.Text;
using System.Reflection;

using CommandLine;

namespace Tile2MBTIles
{
	class MainClass
	{
		enum MBTilesErrorType {
			BAD_ARGUMENT = 1,
			NONEXIST_PATH,
			EXIST_OUTPUT,
			CANNOT_CREATE
		};

		static Dictionary<MBTilesErrorType,string> ErrorMessage = new Dictionary<MBTilesErrorType,string>(){
			{MBTilesErrorType.BAD_ARGUMENT,       "コマンドライン引数の解析に失敗。"},
			{MBTilesErrorType.NONEXIST_PATH,      "探索パスが存在しません。"},
			{MBTilesErrorType.EXIST_OUTPUT,       "出力ファイルが既に存在します。"},
			{MBTilesErrorType.CANNOT_CREATE,      "出力ファイルが作成できません。"}
		};

		public static void Main (string[] args)
		{
			var opts = new Options();
			bool isSuccess = CommandLine.Parser.Default.ParseArguments(args, opts);
			if (!isSuccess)
				HandleError (MBTilesErrorType.BAD_ARGUMENT);

			if (opts.help) {
				Console.WriteLine ("Tile2MBTiles.exe [-p searchPath] [-o outputFile]");
				Environment.Exit(0);
			} 

			try {
				var searchPath = opts.searcnPath;
				//パス取得（カレントパス）
				if (searchPath == null) {
					var assemble = Assembly.GetEntryAssembly();
					searchPath = Path.GetDirectoryName (assemble.Location) + "/";
				}
				if (!Directory.Exists(searchPath))
					HandleError(MBTilesErrorType.NONEXIST_PATH);
					
				var outputFile = opts.outputFile;
				//DB名の生成とコネクション文字列生成
				if (outputFile == null) {
					outputFile = searchPath + "map.mbtiles";
				}
				if (File.Exists(outputFile))
					HandleError(MBTilesErrorType.EXIST_OUTPUT);

				var ws = File.Create (outputFile);
				if (ws == null) 
					HandleError(MBTilesErrorType.CANNOT_CREATE);
				ws.Close();
				var conString = "URI=" + new System.Uri(outputFile).AbsoluteUri;

				using (var conn = new SqliteConnection(conString)) {
					//スキーマ作成
					conn.Open();
					var comm = conn.CreateCommand();
					comm.CommandText = "CREATE TABLE map ( zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_id TEXT, grid_id TEXT );";
					comm.ExecuteNonQuery();
					comm.CommandText = "CREATE TABLE grid_key ( grid_id TEXT, key_name TEXT );";
					comm.ExecuteNonQuery();
					comm.CommandText = "CREATE TABLE keymap ( key_name TEXT, key_json TEXT );";
					comm.ExecuteNonQuery();
					comm.CommandText = "CREATE TABLE grid_utfgrid ( grid_id TEXT, grid_utfgrid BLOB );";
					comm.ExecuteNonQuery();
					comm.CommandText = "CREATE TABLE images ( tile_data blob, tile_id text );";
					comm.ExecuteNonQuery();
					comm.CommandText = "CREATE TABLE metadata ( name text, value text );";
					comm.ExecuteNonQuery();
					comm.CommandText = "CREATE UNIQUE INDEX map_index ON map (zoom_level, tile_column, tile_row);";
					comm.ExecuteNonQuery();
					comm.CommandText = "CREATE UNIQUE INDEX grid_key_lookup ON grid_key (grid_id, key_name);";
					comm.ExecuteNonQuery();
					comm.CommandText = "CREATE UNIQUE INDEX keymap_lookup ON keymap (key_name);";
					comm.ExecuteNonQuery();
					comm.CommandText = "CREATE UNIQUE INDEX grid_utfgrid_lookup ON grid_utfgrid (grid_id);";
					comm.ExecuteNonQuery();
					comm.CommandText = "CREATE UNIQUE INDEX images_id ON images (tile_id);";
					comm.ExecuteNonQuery();
					comm.CommandText = "CREATE UNIQUE INDEX name ON metadata (name);";
					comm.ExecuteNonQuery();
					comm.CommandText = "CREATE VIEW tiles AS SELECT map.zoom_level AS zoom_level, map.tile_column AS tile_column, map.tile_row AS tile_row, images.tile_data AS tile_data FROM map JOIN images ON images.tile_id = map.tile_id;";
					comm.ExecuteNonQuery();
					comm.CommandText = "CREATE VIEW grids AS SELECT map.zoom_level AS zoom_level, map.tile_column AS tile_column, map.tile_row AS tile_row, grid_utfgrid.grid_utfgrid AS grid FROM map JOIN grid_utfgrid ON grid_utfgrid.grid_id = map.grid_id;";
					comm.ExecuteNonQuery();
					comm.CommandText = "CREATE VIEW grid_data AS SELECT map.zoom_level AS zoom_level, map.tile_column AS tile_column, map.tile_row AS tile_row, keymap.key_name AS key_name, keymap.key_json AS key_json FROM map JOIN grid_key ON map.grid_id = grid_key.grid_id JOIN keymap ON grid_key.key_name = keymap.key_name;";
					comm.ExecuteNonQuery();

					//タイル画像検索
					var files = GetFiles(
						searchPath, // 検索開始ディレクトリ
						@"[0-9]+/[0-9]+/[0-9]+\.(png|jpe?g)$", // 検索パターン
						SearchOption.AllDirectories); // サブ・ディレクトリ含めない

					foreach (string file in files) {
						var reg = new Regex(@"(?<zoom>[0-9]+)/(?<x>[0-9]+)/(?<y>[0-9]+)\.(png|jpe?g)$",
							RegexOptions.IgnoreCase | RegexOptions.Singleline);
						var match = reg.Match(file);

						if (match != null)
						{
							//画像バイナリ, x, y, zoom取得
							var img   = File.ReadAllBytes(file);
							var x     = int.Parse(match.Groups["x"].Value);
							var y     = int.Parse(match.Groups["y"].Value);
							var zoom  = int.Parse(match.Groups["zoom"].Value);

							//画像ハッシュ取得
							var md5   = new MD5CryptoServiceProvider();
							var bs    = md5.ComputeHash(img);
							md5.Clear();

							var result = new StringBuilder();
							foreach (var b in bs) 
							{
								result.Append(b.ToString("x2"));
							}
							var sres = result.ToString();

							//mapテーブルに挿入
							comm.Parameters.Clear();
							comm.CommandText = "INSERT INTO map (zoom_level, tile_column, tile_row, tile_id) VALUES (@zoom, @x, @y, @id);";
							comm.Parameters.AddWithValue("@zoom", zoom);
							comm.Parameters.AddWithValue("@x",    x);
							comm.Parameters.AddWithValue("@y",    y);
							comm.Parameters.AddWithValue("@id",   sres);
							comm.ExecuteNonQuery();

							//imagesテーブルに挿入
							comm.Parameters.Clear();
							comm.CommandText = "INSERT OR REPLACE INTO images (tile_data, tile_id) VALUES (@blob, @id);";
							comm.Parameters.AddWithValue("@blob", img);
							comm.Parameters.AddWithValue("@id",   sres);
							comm.ExecuteNonQuery();
						} 
					}

					comm.Dispose();
					comm = null;
					conn.Close();
				}
			} catch (Exception ex) {
				Console.Write ("エラー: ");
				Console.WriteLine (ex.Message);
				Environment.Exit (1);
			}
		}

		private static void HandleError (MBTilesErrorType errType) {
			Console.Write ("エラー: ");
			Console.WriteLine (ErrorMessage [errType]);
			Environment.Exit (1);
		}

		//Regex version
		public static IEnumerable<string> GetFiles(string path, 
			string searchPatternExpression = "",
			SearchOption searchOption = SearchOption.TopDirectoryOnly)
		{
			Regex reSearchPattern = new Regex(searchPatternExpression);
			return Directory.EnumerateFiles (path, "*", searchOption)
					.Where (file =>
						reSearchPattern.IsMatch (file));
		}

		// Takes same patterns, and executes in parallel
		public static IEnumerable<string> GetFiles(string path, 
			string[] searchPatterns, 
			SearchOption searchOption = SearchOption.TopDirectoryOnly)
		{
			return searchPatterns.AsParallel()
					.SelectMany(searchPattern => 
						Directory.EnumerateFiles(path, searchPattern, searchOption));
		}
	}

	class Options {
		[CommandLine.Option('p',"path", DefaultValue=null)]
		public string searcnPath
		{
			get;
			set;
		}

		[CommandLine.Option('o',"output", DefaultValue=null)]
		public string outputFile
		{
			get;
			set;
		}

		[CommandLine.Option('h',"help", DefaultValue=false)]
		public bool help
		{
			get;
			set;
		}
	}
}