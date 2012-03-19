package com.raphfrk.bukkit.loginqueue2;

import java.util.Iterator;

import org.bukkit.entity.Player;
import org.bukkit.event.Event;
import org.bukkit.event.EventException;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerLoginEvent;
import org.bukkit.event.player.PlayerLoginEvent.Result;
import org.bukkit.plugin.EventExecutor;

import com.raphfrk.bukkit.loginqueue2.LoginQueue.PlayerInfo;

public class LoginQueuePlayerListener implements EventExecutor, Listener {

	LoginQueue p;

	LoginQueuePlayerListener(LoginQueue p) {
		this.p = p;
	}
	
	public void execute(Listener listener, Event event) throws EventException {
		if (event instanceof PlayerLoginEvent && listener == this) {
			this.onPlayerLogin((PlayerLoginEvent)event);
		}
	}

	public void onPlayerLogin(PlayerLoginEvent event) {

		Player player = event.getPlayer();
		String playerName = player.getName();

		if(player.isOp() || p.reserveList.contains(playerName) || p.fileReserveList.contains(playerName)) {
			return;
		}
		
		boolean serverFullWithQueue = p.trackedPlayers.size() >= p.serverMaxPopulation.get();

		PlayerInfo info = p.trackedPlayers.get(playerName);
		if(info == null) {
			info = new PlayerInfo();
			info.queued.set(true);
			p.trackedPlayers.put(playerName, info);
		} else if(info.eligible.get() || info.connect.get()) {
			return;
		}
		info.polledQueue.set(System.currentTimeMillis());

		if(!info.eligible.get() && serverFullWithQueue) {
			int pos = getPosition(playerName) - p.skipQueue.get();
			if(pos >= p.skipQueue.get()) {
				String population =  Integer.toString(p.serverPopulation.get());
				String maxPopulation = Integer.toString(p.serverMaxPopulation.get());
				String position = Integer.toString(pos);
				String message = p.fullMessage.get();
				message = message.replaceAll("%pop", population);
				message = message.replaceAll("%max", maxPopulation);
				message = message.replaceAll("%pos", position);
				event.disallow(Result.KICK_FULL, message);
				return;
			}
		}
		
		if(!info.eligible.get()) {
			p.serverPopulation.incrementAndGet();
		}
		
		synchronized(p.trackedPlayers) {		
			info.eligible.set(false);
			info.queued.set(false);
			info.connect.set(true);
			info.becameEligible.set(System.currentTimeMillis());
			info.lastPollDetected.set(System.currentTimeMillis());
		}
		
	}


	Integer getPosition(String playerName) {
		int pos = 0;
		synchronized(p.trackedPlayers) {
			Iterator<String> itr = p.trackedPlayers.keySet().iterator();
			while(itr.hasNext()) {
				String name = itr.next();
				PlayerInfo info = p.trackedPlayers.get(name);
				if(name.equals(playerName)) {
					return pos;
				}
				if(info.queued.get()) {
					pos++;
				}
			}
		}
		return pos;
	}

}