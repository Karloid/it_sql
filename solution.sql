CREATE SCHEMA my;
CREATE TABLE "my"."kv_double"
(
    "key"   TEXT             NOT NULL PRIMARY KEY,
    "value" DOUBLE PRECISION NOT NULL
);

CREATE TABLE "my"."kv_int"
(
    "key"   TEXT NOT NULL PRIMARY KEY,
    "value" int  NOT NULL
);

CREATE TABLE "my"."kv_text"
(
    "key"   TEXT NOT NULL PRIMARY KEY,
    "value" int  NOT NULL
);

-- Function to get an integer value by key from "kv_int" table
CREATE OR REPLACE FUNCTION getInt(keyInp TEXT)
    RETURNS int AS
$BODY$
DECLARE
    result int;
BEGIN
    SELECT "value"
    INTO result
    FROM "my"."kv_int"
    WHERE "key" = keyInp;

    RETURN result;
END;
$BODY$
    LANGUAGE plpgsql;

-- Procedure to set an integer value by key in "kv_int" table
CREATE OR REPLACE PROCEDURE setInt(keyInp TEXT, valueInp int) AS
$BODY$
BEGIN
    if valueInp is null then
        delete from "my"."kv_int" where "key" = keyInp;
        return;
    end if;
    INSERT INTO "my"."kv_int" ("key", "value")
    VALUES (keyInp, valueInp)
    ON CONFLICT ("key") DO UPDATE SET "value" = EXCLUDED."value";
END;
$BODY$
    LANGUAGE plpgsql;

-- Function to get a double value by key from "kv_double" table
CREATE OR REPLACE FUNCTION getDouble(keyInp TEXT)
    RETURNS double precision AS
$BODY$
DECLARE
    result double precision;
BEGIN
    SELECT "value"
    INTO result
    FROM "my"."kv_double"
    WHERE "key" = keyInp;

    RETURN result;
END;
$BODY$
    LANGUAGE plpgsql;

-- Procedure to set a double value by key in "kv_double" table
CREATE OR REPLACE PROCEDURE setDouble(keyInp TEXT, valueInp double precision) AS
$BODY$
BEGIN
    if valueInp is null then
        delete from "my"."kv_double" where "key" = keyInp;
        return;
    end if;

    INSERT INTO "my"."kv_double" ("key", "value")
    VALUES (keyInp, valueInp)
    ON CONFLICT ("key") DO UPDATE SET "value" = EXCLUDED."value";
END;
$BODY$
    LANGUAGE plpgsql;

-- Function to get a text value by key from "kv_text" table
CREATE OR REPLACE FUNCTION getText(keyInp TEXT)
    RETURNS text AS
$BODY$
DECLARE
    result text;
BEGIN
    SELECT "value"
    INTO result
    FROM "my"."kv_text"
    WHERE "key" = keyInp;

    RETURN result;
END;
$BODY$
    LANGUAGE plpgsql;

-- Procedure to set a text value by key in "kv_text" table
CREATE OR REPLACE PROCEDURE setText(keyInp TEXT, valueInp text) AS
$BODY$
BEGIN
    if valueInp is null then
        delete from "my"."kv_text" where "key" = keyInp;
        return;
    end if;
    INSERT INTO "my"."kv_text" ("key", "value")
    VALUES (keyInp, valueInp)
    ON CONFLICT ("key") DO UPDATE SET "value" = EXCLUDED."value";
END;
$BODY$
    LANGUAGE plpgsql;


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
    currentTime                              double precision;
    myMoney                                  double precision;
    oppMoney                                 double precision;
    ship                                     record;
    itemsMeta                                record;
    currentContract                          record;
    currentContractDetails                   record;
    contractDraft                            record;
    vendor                                   record;
    contractDraftFulfilledQty                double precision;
    contractDraftRemainToFulfillQty          double precision;
    vendorQtyToBuy                           double precision;
    additionalContractQty                    double precision;
    additionalContractCurrentCargoQty        double precision;
    additionalContractCurrentMissingCargoQty double precision;
    additionalContractCurrentStorageQty      double precision;
    parkedShip                               record;
    currentIslandInfo                        record;
    islandToLoadInfo                         record;
    existingItemStorageQty                   double precision;
    remainingItemsStoredInfo                 record;
    additionalContractProposeInfo            record;
    additionalContractActualInfo             record;
    myTransferingShips                       integer;
    debugg                                   boolean := true;
    mainContractOfferId                      integer;
    debugContracts                           record;
    tmpInt                                   integer;
    shouldMove                               integer;
BEGIN
    select game_time into currentTime from world.global;
    select money into myMoney from world.players where id = player_id;
    select money into oppMoney from world.players where id <> player_id order by id limit 1;
    if debugg then
        raise notice '[PLAYER %] time: % and money: % opp: %', player_id, currentTime, myMoney, oppMoney;
    end if;

    -- TODO pick best contract to sell if nothing yet

    select contract into tmpInt from events.contract_started where offer = getInt('main_contract_offer');
    if tmpInt is not null then
        call setInt('main_contract', tmpInt);
    end if;

    select *
    into currentContract
    from world.contracts c
    where c.player = player_id
      and c.id = getInt('main_contract');

    if currentContract is null then
        if debugg then
            raise notice '[PLAYER %] no contract yet, try find contract, old main contract %', player_id, getInt('main_contract');
        end if;

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
            -- finding customer for remaining stuff
            if debugg then
                raise notice '[PLAYER %] found remaining item % qty % try find customer for it', player_id,
                    remainingItemsStoredInfo.id, remainingItemsStoredInfo.remaining;
            end if;

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
            select best_item.id as item_id, c.*, best_item.has_not_empty_vendors, best_item.total_profit
            into contractDraft
            from (select *
                  from (select *,
                               (select count(*)
                                from world.contractors c
                                where c.item = i.id
                                  and c.type = 'vendor')                                    vendors,
                               (select count(*)
                                from world.contractors c
                                where c.item = i.id
                                  and c.type = 'customer')                                  customers,
                               -- possible profit
                               (select max(c.price_per_unit)
                                from world.contractors c
                                where c.item = i.id
                                  and c.type = 'customer') - (select min(c.price_per_unit)
                                                              from world.contractors c
                                                              where c.item = i.id
                                                                and c.type = 'vendor')      max_price_diff,
                               (select sum(c.price_per_unit * c.quantity)
                                from world.contractors c
                                where c.item = i.id
                                  and c.type = 'customer') - (select sum(c.price_per_unit * c.quantity)
                                                              from world.contractors c
                                                              where c.item = i.id
                                                                and c.type = 'vendor')      total_profit,
                               (select sum(c.quantity)
                                from world.contractors c
                                where c.item = i.id
                                  and c.type = 'vendor'
                                  and c.price_per_unit < (select max(c.price_per_unit)
                                                          from world.contractors c
                                                          where c.item = i.id
                                                            and c.type = 'customer')) > 1.0 has_not_empty_vendors,
                               -- find what quantity of items can be actually sold to best customer
                               (select min(c.quantity)
                                from world.contractors c
                                where c.item = i.id
                                  and c.type = 'vendor') * (select max(c.price_per_unit)
                                                            from world.contractors c
                                                            where c.item = i.id
                                                              and c.type = 'customer')      max_sum_value


                        from world.items i) d
                  where d.has_not_empty_vendors = true

                  order by d.total_profit desc
                  limit 1) best_item,
                 world.contractors c
            where c.item = best_item.id
              and c.type = 'customer'
              and c.price_per_unit = (select max(c.price_per_unit)
                                      from world.contractors c
                                      where c.item = best_item.id
                                        and c.type = 'customer');
            if debugg then
                raise notice '[PLAYER %] found best item % qty % price % has_not_empty_vendors % total_profit %',
                    player_id, contractDraft.item_id,
                    contractDraft.quantity, contractDraft.price_per_unit, contractDraft.has_not_empty_vendors,
                    contractDraft.total_profit;
            end if;

        end if;

        if contractDraft is null then
            if debugg then raise notice '[PLAYER %] no contract found, need wait for 5 sec', player_id; end if;
            insert into actions.wait (until) values (currentTime + 5);
            return;
        end if;


        if debugg then
            raise notice '[PLAYER %] best contract possible is item_id % contractor % qty % price_per_unit % max-sum-value %',
                player_id, contractDraft.item_id, contractDraft.id, contractDraft.quantity, contractDraft.price_per_unit,
                contractDraft.quantity * contractDraft.price_per_unit;
        end if;

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
                if debugg then
                    raise notice '[PLAYER %] vendor % contractDraftRemainToFulfillQty % vendor.quantity %', player_id, vendor.id,
                        contractDraftRemainToFulfillQty, vendor.quantity;
                end if;

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
                    if debugg then
                        raise notice '[PLAYER %] buying % items from vendor % by price % sum-buy-value %',
                            player_id, vendorQtyToBuy, vendor.id, vendor.price_per_unit, vendorQtyToBuy * vendor.price_per_unit;
                    end if;

                    insert into actions.offers (contractor, quantity) values (vendor.id, vendorQtyToBuy);
                end if;

            end loop;

        -- insert sell contract from draftContract
        -- notice about actual placed contract
        if debugg then
            raise notice '[PLAYER %] actual placed contract, contractor % quantity % item % price % sum-sell-value %',
                player_id, contractDraft.id, contractDraftFulfilledQty, contractDraft.item_id, contractDraft.price_per_unit, contractDraft.quantity * contractDraft.price_per_unit;
        end if;
        insert into actions.offers (contractor, quantity)
        values (contractDraft.id, contractDraftFulfilledQty - 0.000000001)
        returning id into mainContractOfferId;

        call setInt('main_contract_offer', mainContractOfferId);

        return;
    end if;

    -- TODO buy enough items if something missing

    select item, island, price_per_unit
    into currentContractDetails
    from world.contractors
    where id = currentContract.contractor;

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
    where currentContractDetails.item = data.id;

    select count(*)
    into myTransferingShips
    from world.transferring_ships ts,
         world.ships s
    where ts.ship = s.id
      and s.player = player_id;

    if remainingItemsStoredInfo.remaining < currentContract.quantity and myTransferingShips = 0 then
        if debugg then
            raise notice '[PLAYER %] !ERROR! not enough items % need % stored % diff % buy remaining',
                player_id, currentContractDetails.item, currentContract.quantity, remainingItemsStoredInfo.remaining, currentContract.quantity - remainingItemsStoredInfo.remaining;
        end if;
        -- todo handle case if no remaining vendors
        contractDraftRemainToFulfillQty := currentContract.quantity + 0.000000001;

        select coalesce((select sum(c.quantity)
                         from world.cargo c
                         where c.item = currentContractDetails.item
                           and c.ship in (select s.id from world.ships s where s.player = player_id)), 0.0) +
               coalesce((select sum(c.quantity)
                         from world.storage c
                         where c.item = currentContractDetails.item
                           and c.player = player_id), 0.0)
        into existingItemStorageQty;

        contractDraftFulfilledQty := existingItemStorageQty;
        contractDraftRemainToFulfillQty := contractDraftRemainToFulfillQty - contractDraftFulfilledQty;

        for vendor in
            select *
            from world.contractors c
            where c.type = 'vendor'
              and c.item = currentContractDetails.item
              and c.price_per_unit <= currentContractDetails.price_per_unit
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
                    if debugg then
                        raise notice '[PLAYER %] buying % items from vendor % by price % sum-buy-value %',
                            player_id, vendorQtyToBuy, vendor.id, vendor.price_per_unit, vendorQtyToBuy * vendor.price_per_unit;
                    end if;

                    insert into actions.offers (contractor, quantity) values (vendor.id, vendorQtyToBuy);
                end if;

            end loop;
        return;
    end if;

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

           -- TODO MOVE PENDING CHECKS OUTSIDE OF PARKED SHIP LOOP => THIS WILL FIX BUG PROBABLY
            if getInt('additionalContractOffer_ship_' || parkedShip.id) is not null then
                if debugg then
                    raise notice '[PLAYER %] ship % has additional pending offer %',
                        player_id, parkedShip.id, getInt('additionalContractOffer_ship_' || parkedShip.id);
                end if;
            end if;

            select contract
            into tmpInt
            from events.contract_started
            where offer = getInt('additionalContractOffer_ship_' || parkedShip.id);

            if tmpInt is not null then
                if debugg then
                    raise notice '[PLAYER %] ship % offer % was accepted additional contract %',
                        player_id, parkedShip.id, getInt('additionalContractOffer_ship_' || parkedShip.id), tmpInt;
                end if;
                call setInt('additionalContractId_ship_' || parkedShip.id, tmpInt);
            elseif getInt('additionalContractOffer_ship_' || parkedShip.id) is not null then
                if debugg then
                    raise notice '[PLAYER %] WARN ship % additional offer % was rejected!',
                        player_id, parkedShip.id, getInt('additionalContractOffer_ship_' || parkedShip.id);
                end if;
            end if;
            call setInt('additionalContractOffer_ship_' || parkedShip.id, null);

            select c.id,
                   c.quantity,
                   contractors.item,
                   contractors.island                                   to_island,
                   coalesce((select sum(cargo.quantity)
                             from world.cargo cargo
                             where cargo.ship = parkedShip.ship
                               and cargo.item = contractors.item), 0.0) currentCargo
            into additionalContractActualInfo
            from world.contracts c,
                 world.contractors contractors
            where c.id = getInt('additionalContractId_ship_' || parkedShip.id)
              and contractors.id = c.contractor;


            select coalesce(sum(storage.quantity), 0.0) storageItemQty
            into currentIslandInfo
            from world.storage storage
            where storage.island = parkedShip.island
              and storage.item = currentContractDetails.item
              and storage.player = player_id;

            if debugg then
                raise notice '[PLAYER %] ship % is on  currentCargo % island.id % island.storageItemQty %',
                    player_id, parkedShip.ship, parkedShip.currentCargo,parkedShip.island, currentIslandInfo.storageItemQty;
            end if;

            if additionalContractActualInfo is not null and
               parkedShip.island = additionalContractActualInfo.to_island and
               additionalContractActualInfo.quantity > 0.0 then
                if debugg then
                    raise notice '[PLAYER %] ship % is on island % and has additional % items of type % going to transfer, contract id %',
                        player_id, parkedShip.ship, parkedShip.island,
                        additionalContractActualInfo.currentCargo,
                        additionalContractActualInfo.item,
                        additionalContractActualInfo.id;
                end if;
                insert into actions.transfers (ship, item, quantity, direction)
                values (parkedShip.ship,
                        additionalContractActualInfo.item,
                        additionalContractActualInfo.currentCargo,
                        'unload');
            elseif parkedShip.island = currentContractDetails.island and parkedShip.currentCargo > 0.0 then
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
                    -- try find suitable order

                    shouldMove := 1;


                    if additionalContractActualInfo is not null then
                        select coalesce(sum(cargo.quantity), 0.0) cargoQty
                        into additionalContractCurrentCargoQty
                        from world.cargo cargo
                        where cargo.ship = parkedShip.ship
                          and cargo.item = additionalContractActualInfo.item;

                        select coalesce(sum(storage.quantity), 0.0) cargoQty
                        into additionalContractCurrentStorageQty
                        from world.storage storage
                        where storage.island = parkedShip.island
                          and storage.item = additionalContractActualInfo.item;

                        if additionalContractCurrentCargoQty >= additionalContractActualInfo.quantity then
                            if debugg then
                                raise notice '[PLAYER %] ship % is on island % ready for additional contracts, will go cargoQty %',
                                    player_id, parkedShip.ship, parkedShip.island, additionalContractCurrentCargoQty;
                            end if;
                            shouldMove := 1;
                        else
                            shouldMove := 0;
                            additionalContractCurrentMissingCargoQty :=
                                        additionalContractActualInfo.quantity - additionalContractCurrentCargoQty;

                            if additionalContractCurrentStorageQty >= additionalContractCurrentMissingCargoQty then
                                insert into actions.transfers (ship, item, quantity, direction)
                                values (parkedShip.ship,
                                        additionalContractActualInfo.item,
                                        additionalContractCurrentMissingCargoQty,
                                        'load');
                            else
                                insert into actions.offers (contractor, quantity)
                                values (additionalContractProposeInfo.vendor_id,
                                        additionalContractCurrentMissingCargoQty);
                            end if;
                        end if;

                    else
                        select vendors.id         vendor_id,
                               customers.id       customer_id,
                               vendors.item,
                               vendors.quantity   vqty,
                               customers.quantity cqty
                        into additionalContractProposeInfo
                        from world.contractors vendors,
                             world.contractors customers
                        where vendors.type = 'vendor'
                          and vendors.quantity > 0.0
                          and vendors.island = parkedShip.island
                          and customers.type = 'customer'
                          and customers.item = vendors.item
                          and customers.price_per_unit >= vendors.price_per_unit
                          and customers.quantity > 0.0
                          and customers.island = islandToLoadInfo.island
                        order by customers.price_per_unit * LEAST(vendors.quantity, customers.quantity) -
                                 vendors.price_per_unit * LEAST(vendors.quantity, customers.quantity) desc
                        limit 1;

                        if additionalContractProposeInfo is not null then

                            select least(ships.capacity
                                             - coalesce(
                                                 (select sum(cargo.quantity)
                                                  from world.cargo cargo
                                                  where cargo.ship = parkedShip.id),
                                                 0.0),
                                         additionalContractProposeInfo.vqty,
                                         additionalContractProposeInfo.cqty)
                            into additionalContractQty
                            from world.ships ships
                            where ships.id = parkedShip.id;

                            if additionalContractQty > 1.0 then
                                insert into actions.offers (contractor, quantity)
                                values (additionalContractProposeInfo.vendor_id, additionalContractQty);
                                insert into actions.offers (contractor, quantity)
                                values (additionalContractProposeInfo.customer_id, additionalContractQty)
                                returning id into tmpInt;
                                call setInt('additionalContractOffer_ship_' || parkedShip.id, tmpInt);
                                if debugg then
                                    raise notice '[PLAYER %] ship % is on island % and can do additional job item % max qty % wait for confirmation offer %',
                                        player_id, parkedShip.ship,
                                        parkedShip.island,additionalContractProposeInfo.item,
                                        LEAST(additionalContractProposeInfo.vqty,
                                              additionalContractProposeInfo.cqty),
                                        tmpInt;
                                end if;
                            end if;
                        end if;
                    end if;


                    if shouldMove = 1 then
                        if debugg then
                            raise notice '[PLAYER %] ship % is on island % going to move to island %',
                                player_id, parkedShip.ship, parkedShip.island, islandToLoadInfo.island;
                        end if;
                        call moveToTheNextIsland(player_id, parkedShip.id, islandToLoadInfo.island);
                    end if;
                else
                    -- TODO BUY HERE?
                    if debugg then
                        raise notice '[PLAYER %] !ERROR! no islands with items %', player_id, currentContractDetails.item;
                    end if;
                end if;

            else
                if debugg then
                    raise notice '[PLAYER %] !ERROR! no action found for ship % island % currentCargo %',
                        player_id, parkedShip.id, parkedShip.island, parkedShip.currentCargo;
                end if;
            end if;

        end loop;

    -- TODO move empty ships to islands with needed items

    -- TODO load parked ships

    -- TODO move loaded ships to islands with final customer

    -- TODO unload ships completely


END
$$;