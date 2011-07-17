package com.raphfrk.bukkit.loginqueue2;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.bukkit.Server;
import org.bukkit.entity.Player;
import org.bukkit.event.Event.Priority;
import org.bukkit.event.Event.Type;
import org.bukkit.plugin.java.JavaPlugin;

public class LoginQueue extends JavaPlugin {

	Logger log = Logger.getLogger("Minecraft");

	LoginQueue p = this;

	LoginQueuePlayerListener playerListener = new LoginQueuePlayerListener(this);

	AtomicInteger logoutDwellTime = new AtomicInteger(120);
	AtomicInteger queueDwellTime = new AtomicInteger(600);
	AtomicInteger eligibleDwellTime = new AtomicInteger(300);
	AtomicInteger period = new AtomicInteger(30);

	AtomicBoolean sqlEnabled = new AtomicBoolean(false);

	AtomicString sqlDatabase = new AtomicString();
	AtomicString sqlUser = new AtomicString();
	AtomicString sqlPass = new AtomicString();

	AtomicInteger serverMaxPopulation = new AtomicInteger();
	AtomicInteger skipQueue = new AtomicInteger();

	AtomicString fullMessage = new AtomicString();

	Map<String,PlayerInfo> trackedPlayers = Collections.synchronizedMap(new LinkedHashMap<String,PlayerInfo>());
	Set<String> reserveList = Collections.synchronizedSet(new HashSet<String>());
	Set<String> fileReserveList = new HashSet<String>();

	AtomicInteger serverPopulation = new AtomicInteger(0);

	AtomicString reserveFileLocation = new AtomicString();

	PlayerPollingTimer pollingThread;

	public void onDisable() {

		if(pollingThread != null) {
			while(pollingThread.isAlive()) {
				pollingThread.interrupt();
				try {
					pollingThread.join(500);
				} catch (InterruptedException e) {
				}
			}
		}

	}

	public void onEnable() {

		log.info("Login Queue Started");

		File file = getDataFolder();

		if(!file.exists()) {
			file.mkdirs();
		}

		MyPropertiesFile pf = new MyPropertiesFile(new File(file, "login_queue.txt").toString());
		pf.load();

		logoutDwellTime.set(pf.getInt("logout_dwell_timeout", 120));
		queueDwellTime.set(pf.getInt("queue_dwell_timeout", 600));
		eligibleDwellTime.set(pf.getInt("eligible_dwell_timeout", 300));
		period.set(pf.getInt("polling_period", 300));

		serverMaxPopulation.set(pf.getInt("server_max_population", 5));
		skipQueue.set(pf.getInt("skip_queue", 0));

		sqlEnabled.set(pf.getBoolean("sql_enabled", false));

		reserveFileLocation.set(pf.getString("reserve_file", "reservelist.txt"));

		fullMessage.set(pf.getString("kick_message", "The server is full, %pop of %max, you are in position %pos"));

		sqlDatabase.set(pf.getString("sql_database", "jdbc:mysql://localhost:3306/loginqueue"));
		sqlUser.set(pf.getString("sql_username", "root"));
		sqlPass.set(pf.getString("sql_pass", ""));

		pf.save();

		getServer().getPluginManager().registerEvent(Type.PLAYER_LOGIN, playerListener, Priority.Normal, this);

		if(sqlEnabled.get()) {
			Connection conn = null;
			try {

				try {
					//log.info("Db = " + sqlDb + "Password: " + sqlPass + " user: " + sqlUser);
					conn = DriverManager.getConnection(sqlDatabase.get() + "?autoReconnect=true&user=" + sqlUser.get() + "&password=" + sqlPass.get());
				} catch (SQLException ex) { 
					ex.printStackTrace();
				}

				Statement stmt = conn.createStatement();

				stmt.execute("DROP TABLE IF EXISTS loginqueue");

				String QueryString = "CREATE TABLE IF NOT EXISTS loginqueueplayertable (" +
				"playername VARCHAR(255) PRIMARY KEY, " +
				"connected INT, " + 
				"queued INT, " +
				"eligible INT, " +
				"lastConnectPoll BIGINT, " +
				"joinedQueue BIGINT, " +
				"polledQueue BIGINT, " +
				"becameEligible BIGINT)";
				stmt.executeUpdate(QueryString);

				QueryString = "CREATE TABLE IF NOT EXISTS reservelist ( name VARCHAR(255) )";
				stmt.executeUpdate(QueryString);

				conn.close();

			} catch (SQLException ex) { 
				ex.printStackTrace();
			}

		}

		pollingThread = new PlayerPollingTimer(getServer(), period.get(), sqlDatabase.get(), sqlUser.get(), sqlPass.get() );
		pollingThread.start();


	}

	void updateReserveFileSafe() {
		getServer().getScheduler().scheduleSyncDelayedTask(p, new Runnable() {
			public void run() {
				updateReserveFile();
			}
		});
	}

	long lastUpdate = 0;

	void updateReserveFile() {

		if(System.currentTimeMillis() < 30000 + lastUpdate) {
			return;
		}

		if( reserveFileLocation == null ) return;

		File reserveFile = new File( getDataFolder(), reserveFileLocation.get() );

		if( !reserveFile.exists() ) {
			ArrayList<String> blank = new ArrayList<String>();
			MiscUtils.stringToFile(blank, reserveFile.toString());
		}

		String[] names = MiscUtils.fileToString(reserveFile.toString());

		for( String name : names ) {
			fileReserveList.add(name);
		}
	}

	class PlayerPollingTimer extends Thread {

		final String newline = System.getProperty("line.separator");

		Connection conn = null;

		final Server server;
		final long period;

		PlayerPollingTimer(Server server, long period, String sqlDb, String sqlUser, String sqlPass) {
			this.server = server;
			this.period = period;
			try {
				if(sqlEnabled.get()) {
					conn = DriverManager.getConnection(sqlDb + "?autoReconnect=true&user=" + sqlUser + "&password=" + sqlPass);
				}
			} catch (SQLException e) {
				e.printStackTrace();
				killed.set(true);
			}
		}

		final AtomicBoolean killed = new AtomicBoolean(false);



		public void run() {

			log.info("[LoginQueue] Starting Poll Timer Thread");

			while(!killed.get()) {

				updateReserveFileSafe();

				Future<List<String>> playerListFuture = server.getScheduler().callSyncMethod(p, new Callable<List<String>>() {

					public List<String> call() {
						Player[] players = server.getOnlinePlayers();
						List<String> list = new ArrayList<String>();
						for(Player player : players) {
							list.add(player.getName());
						}
						return list;
					}
				});

				List<String> playerList = null;
				try {
					playerList = playerListFuture.get(100, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					killed.set(true);
					continue;
				} catch (ExecutionException e) {
					e.printStackTrace();
				} catch (TimeoutException e) {
				}

				long currentTime = System.currentTimeMillis();

				if(playerList != null) {					
					synchronized(trackedPlayers) {

						Iterator<String> itr = trackedPlayers.keySet().iterator();
						int population = 0;
						while(itr.hasNext()) {
							String current = itr.next();
							PlayerInfo player = trackedPlayers.get(current);
							//System.out.println(current + " " + player);
							if(player.connect.get() || player.eligible.get()) {
								population++;
							}
						}
						serverPopulation.set(population);
						itr = trackedPlayers.keySet().iterator();
						int freeSlots = serverMaxPopulation.get() - population;

						while(itr.hasNext()) {
							String current = itr.next();
							PlayerInfo player = trackedPlayers.get(current);
							if(player.queued.get() && freeSlots > 0) {
								freeSlots--;
								player.queued.set(false);
								player.eligible.set(true);
								player.becameEligible.set(currentTime);
							}
							if(player.connect.get() && currentTime - player.lastPollDetected.get() > 1000*logoutDwellTime.get()) {
								itr.remove();
							}
							if(player.queued.get() && currentTime - player.polledQueue.get() > 1000*queueDwellTime.get()) {
								itr.remove();
							}
							if(player.eligible.get() && currentTime - player.becameEligible.get() > 1000*eligibleDwellTime.get()) {
								itr.remove();
							}
						}
					}

					Iterator<String> itr = playerList.iterator();
					while(itr.hasNext()) {
						String current = itr.next();
						PlayerInfo player = trackedPlayers.get(current);
						if(player == null) {
							player = new PlayerInfo();
							player.connect.set(true);
							player.queued.set(false);
							player.eligible.set(false);
							trackedPlayers.put(current, player);
						} else if(!player.connect.get() || player.queued.get() || player.eligible.get()){
							player.connect.set(true);
							player.queued.set(false);
							player.eligible.set(false);
						} 
						synchronized(player) {
							player.lastPollDetected.set(currentTime);
						}
					}

					if(conn != null) {
						try {
							conn.setAutoCommit(false);
							Statement stmt = conn.createStatement();
							stmt.executeUpdate("DELETE FROM loginqueueplayertable");

							synchronized(trackedPlayers) {
								itr = trackedPlayers.keySet().iterator();
								while(itr.hasNext()) {
									String player = itr.next();
									PlayerInfo info = trackedPlayers.get(player);
									if(info != null) {
										String SQLString = 
											"INSERT INTO loginqueueplayertable (playername, connected , queued, eligible, lastConnectPoll, joinedQueue, polledQueue, becameEligible) " +
											"VALUES ('" + 
											player + "', " + 
											(info.connect.get()?1:0) + ", " +
											(info.queued.get()?1:0) + ", " + 
											(info.eligible.get()?1:0) + ", " +
											(info.lastPollDetected.get()) + ", " +
											(info.joinedQueue.get()) + ", " +
											(info.polledQueue.get()) + ", " +
											(info.becameEligible.get()) + ") " + newline;
										//System.out.println(SQLString);	
										stmt.execute(SQLString);
									}
								}
								conn.commit();
								conn.setAutoCommit(true);
								stmt = conn.createStatement();
								ResultSet rs = stmt.executeQuery("SELECT name FROM reservelist");
								LinkedList<String> temp = new LinkedList<String>();
								while(rs.next()) {
									String name = rs.getString("name");
									if(name != null) {
										temp.add(name);
									}
								}
								synchronized(reserveList) {
									reserveList.clear();
									for(String name : temp) {
										reserveList.add(name);
									}
								}
							}
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
				}
				try {
					Thread.sleep(this.period * 1000);
				} catch (InterruptedException e) {
					killed.set(true);
					interrupt();
				}
			}

			log.info("[LoginQueue] Shutting down player polling timer");
			try {
				if(conn!=null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
				log.info("[LoginQueue] SQL Connection shut-down failed");
			}


		}

	}

	static class PlayerInfo {

		PlayerInfo() {

		}

		AtomicBoolean connect = new AtomicBoolean(false);
		AtomicBoolean queued = new AtomicBoolean(true);
		AtomicBoolean eligible = new AtomicBoolean(false);
		AtomicLong lastPollDetected = new AtomicLong(0);
		AtomicLong joinedQueue = new AtomicLong(0);
		AtomicLong polledQueue = new AtomicLong(0);
		AtomicLong becameEligible = new AtomicLong(0);
		
		public synchronized String toString() {
			return (connect.get()?"connect ":"") + 
					(queued.get()?"queued ":"") +
					(eligible.get()?"eligible ":"") + 
					lastPollDetected + " " + joinedQueue + " " + polledQueue + " " + becameEligible;
 		}

	}

	static class AtomicString {

		String value;

		synchronized String get() {
			return value;
		}

		synchronized void set(String value) {
			this.value = value;
		}
		
	}

}
