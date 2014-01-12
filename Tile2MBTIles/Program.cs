using System;
using System.IO;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Linq;
using Mono.Data.Sqlite;
using System.Security.Cryptography;
using System.Text;
using System.Reflection;



namespace Tile2MBTIles
{
	class MainClass
	{
		private static string path = "";

		public static void Main (string[] args)
		{
			try {
				//パス取得（カレントパス）
#if DEBUG
				path = args[0];
#else
				var assemble = Assembly.GetEntryAssembly();
				path = Path.GetDirectoryName (assemble.Location) + "/";
#endif

				//DB名の生成とコネクション文字列生成
				var mbtiles = path + "map.mbtiles";
				File.Create (mbtiles).Close();
				var conString = "URI=" + new System.Uri(mbtiles).AbsoluteUri;

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
						path, // 検索開始ディレクトリ
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
				throw ex;
			}
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
}