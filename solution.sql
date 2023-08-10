create procedure moveToTheNextIsland(player_id integer, ship_id integer, island_id integer) as
$$
begin
    --  raise notice '[PLAYER %] MOVING SHIP % TO ISLAND %', player_id, ship_id, island_id;
    insert into actions.ship_moves (ship, destination) values (ship_id, island_id);
end
$$ language plpgsql;

CREATE PROCEDURE think(player_id INTEGER)
    LANGUAGE PLPGSQL AS
$$
declare
    currentTime                     double precision;
    myMoney                         double precision;
    ship                            record;
    itemsMeta                       record;
    currentContract                 record;
    currentContractDetails          record;
    contractDraft                   record;
    vendor                          record;
    contractDraftFulfilledQty       double precision;
    contractDraftRemainToFulfillQty double precision;
    vendorQtyToBuy                  double precision;
    parkedShip                      record;
    currentIslandInfo               record;
    islandToLoadInfo                record;
    existingItemStorageQty          double precision;
    remainingItemsStoredInfo        record;
BEGIN
    select game_time into currentTime from world.global;
    select money into myMoney from world.players where id = player_id;
    raise notice '[PLAYER %] time: % and money: %', player_id, currentTime, myMoney;

    -- TODO pick best contract to sell if nothing yet

    select * into currentContract from world.contracts where player = player_id;

    if currentContract is null then
        --   raise notice '[PLAYER %] no contract yet, try find contract', player_id;

        -- select largest remaining in storage and try sell it
        select *
        into remainingItemsStoredInfo
        from (select items.id,
                     coalesce((select sum(c.quantity)
                               from world.cargo c
                               where c.item = items.id
                                 and c.ship in (select s.id from world.ships s where s.player = player_id)), 0.0) +
                     coalesce((select sum(c.quantity)
                               from world.storage c
                               where c.item = items.id
                                 and c.player = player_id), 0.0) remaining
              from world.items items) data
        where data.remaining > 1.0
        order by data.remaining desc
        limit 1;

        if remainingItemsStoredInfo is not null then
            raise notice '[PLAYER %] found remaining item % qty % try find customer for it', player_id,
                remainingItemsStoredInfo.id, remainingItemsStoredInfo.remaining;
            select best_item.id as item_id, c.*
            into contractDraft
            from (select remainingItemsStoredInfo.id) best_item,
                 world.contractors c
            where c.item = best_item.id
              and c.type = 'customer'
              and c.price_per_unit = (select max(c.price_per_unit)
                                      from world.contractors c
                                      where c.item = best_item.id
                                        and c.type = 'customer');


        else
            select best_item.id as item_id, c.*
            into contractDraft
            from (select *
                  from (select *,
                               (select count(*)
                                from world.contractors c
                                where c.item = i.id
                                  and c.type = 'vendor')                               vendors,
                               (select count(*)
                                from world.contractors c
                                where c.item = i.id
                                  and c.type = 'customer')                             customers,
                               -- possible profit
                               (select max(c.price_per_unit)
                                from world.contractors c
                                where c.item = i.id
                                  and c.type = 'customer') - (select min(c.price_per_unit)
                                                              from world.contractors c
                                                              where c.item = i.id
                                                                and c.type = 'vendor') max_price_diff,
                               (select sum(c.price_per_unit * c.quantity)
                                from world.contractors c
                                where c.item = i.id
                                  and c.type = 'customer') - (select sum(c.price_per_unit * c.quantity)
                                                              from world.contractors c
                                                              where c.item = i.id
                                                                and c.type = 'vendor') total_profit
                        from world.items i) d

                  order by d.total_profit desc
                  limit 1) best_item,
                 world.contractors c
            where c.item = best_item.id
              and c.type = 'customer'
              and c.price_per_unit = (select max(c.price_per_unit)
                                      from world.contractors c
                                      where c.item = best_item.id
                                        and c.type = 'customer');

        end if;


        raise notice '[PLAYER %] best contract possible is item_id % contractor % qty % price % max-sum-value %',
            player_id, contractDraft.item_id, contractDraft.id, contractDraft.quantity, contractDraft.price_per_unit,
            contractDraft.quantity * contractDraft.price_per_unit;

        -- todo handle case if no remaining vendors
        contractDraftRemainToFulfillQty := contractDraft.quantity + 0.000000001;
        contractDraftFulfilledQty := 0.0;

        select coalesce((select sum(c.quantity)
                         from world.cargo c
                         where c.item = contractDraft.item
                           and c.ship in (select s.id from world.ships s where s.player = player_id)), 0.0) +
               coalesce((select sum(c.quantity)
                         from world.storage c
                         where c.item = contractDraft.item
                           and c.player = player_id), 0.0)
        into existingItemStorageQty;

        contractDraftFulfilledQty := existingItemStorageQty;
        contractDraftRemainToFulfillQty := contractDraftRemainToFulfillQty - contractDraftFulfilledQty;

        for vendor in
            select *
            from world.contractors c
            where c.type = 'vendor'
              and c.item = contractDraft.item_id
              and c.price_per_unit <= contractDraft.price_per_unit
            order by c.price_per_unit asc
            loop
                --  raise notice '[PLAYER %] vendor % has % items', player_id, vendor.id, vendor.quantity;
                vendorQtyToBuy := 0.0;

                if vendor.quantity >= contractDraftRemainToFulfillQty then
                    vendorQtyToBuy := contractDraftRemainToFulfillQty;
                    contractDraftRemainToFulfillQty := 0.0;
                else
                    vendorQtyToBuy := vendor.quantity;
                    contractDraftRemainToFulfillQty := contractDraftRemainToFulfillQty - vendor.quantity;
                end if;
                contractDraftFulfilledQty := contractDraftFulfilledQty + vendorQtyToBuy;

                if vendorQtyToBuy > 0.0 then
                -- insert to offers contractor and quantity
                -- notice about buying stuff
                raise notice '[PLAYER %] buying % items from vendor % by price % sum-buy-value %',
                    player_id, vendorQtyToBuy, vendor.id, vendor.price_per_unit, vendorQtyToBuy * vendor.price_per_unit;
                insert into actions.offers (contractor, quantity) values (vendor.id, vendorQtyToBuy);
                end if;
                
            end loop;

        -- insert sell contract from draftContract
        -- notice about actual placed contract
        raise notice '[PLAYER %] actual placed contract, contractor % quantity % item % price % sum-sell-value %',
            player_id, contractDraft.id, contractDraftFulfilledQty, contractDraft.item_id, contractDraft.price_per_unit, contractDraft.quantity * contractDraft.price_per_unit;
        insert into actions.offers (contractor, quantity) values (contractDraft.id, contractDraftFulfilledQty - 0.000000001);

        return;
    end if;

    -- TODO buy enough items if something missing

    select item, island
    into currentContractDetails
    from world.contractors
    where id = currentContract.contractor;


    for parkedShip in
        select *,
               coalesce((select sum(cargo.quantity)
                         from world.cargo cargo
                         where cargo.ship = ps.ship
                           and cargo.item = currentContractDetails.item), 0.0) currentCargo
        from world.parked_ships ps,
             world.ships s
        where s.id = ps.ship
          and s.player = player_id
          and s.id not in (select ts.ship from world.transferring_ships ts)
        loop
            --    raise notice '[PLAYER %] ship % is on island %', player_id, parkedShip.ship, parkedShip.island;

            select sum(storage.quantity) storageItemQty
            into currentIslandInfo
            from world.storage storage
            where storage.island = parkedShip.island
              and storage.item = currentContractDetails.item
              and storage.player = player_id;

            if parkedShip.island = currentContractDetails.island and parkedShip.currentCargo > 0.0 then
                -- raise unload notice
                --    raise notice '[PLAYER %] ship % is on island % and has % items going to unload', player_id, parkedShip.ship, parkedShip.island, parkedShip.currentCargo;

                insert into actions.transfers (ship, item, quantity, direction)
                values (parkedShip.ship,
                        currentContractDetails.item,
                        parkedShip.currentCargo,
                        'unload');
            elseif parkedShip.island <> currentContractDetails.island and parkedShip.currentCargo > 0.0 then
                --     raise notice '[PLAYER %] ship % is on island % and has % items going to transfer', player_id, parkedShip.ship, parkedShip.island, parkedShip.currentCargo;

                call moveToTheNextIsland(player_id, parkedShip.id, currentContractDetails.island);

            elseif parkedShip.island <> currentContractDetails.island and parkedShip.currentCargo = 0.0
                and currentIslandInfo.storageItemQty > 0.0 then
                -- load ship with items
                --  raise notice '[PLAYER %] ship % is on island % and has % items going to load', player_id, parkedShip.ship, parkedShip.island, parkedShip.currentCargo;

                insert into actions.transfers (ship, item, quantity, direction)
                values (parkedShip.ship,
                        currentContractDetails.item,
                        currentIslandInfo.storageItemQty,
                        'load');
            elseif parkedShip.currentCargo = 0.0
                and (currentIslandInfo.storageItemQty = 0.0 or parkedShip.island = currentContractDetails.island) then
                -- go to island with biggest qty of items

                select *
                into islandToLoadInfo
                from world.storage
                where item = currentContractDetails.item
                  and player = player_id
                  and quantity > 0.0
                  and island <> currentContractDetails.island
                order by quantity desc
                limit 1;

                -- check for null
                if islandToLoadInfo is not null then
                    --    raise notice '[PLAYER %] ship % is on island % and has % items going to move', player_id, parkedShip.ship, parkedShip.island, parkedShip.currentCargo;
                    call moveToTheNextIsland(player_id, parkedShip.id, islandToLoadInfo.island);
                else
                    --     raise notice '[PLAYER %] !ERROR! no islands with items %', player_id, currentContractDetails.item;
                end if;

            else
                /*  raise notice '[PLAYER %] !ERROR! no action found for ship % island % currentCargo %',
                      player_id, parkedShip.id, parkedShip.island, parkedShip.currentCargo;*/
            end if;

        end loop;

    -- TODO move empty ships to islands with needed items

    -- TODO load parked ships

    -- TODO move loaded ships to islands with final customer

    -- TODO unload ships completely


END
$$;