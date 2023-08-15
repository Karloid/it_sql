--set session game.const.transfer_time_per_unit = 1.0;

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

create table "my"."offers"
(
    "offer" integer          not null,
    "time"  double precision not null
);

CREATE TYPE "my"."ship_state" AS ENUM ('idle', 'moving_to_load', 'wait_for_load_finish', 'moving_to_unload');
create table "my"."ship_states"
(
    "ship"  integer          not null primary key,
    "state" my.ship_state    not null,
    "time"  double precision not null
);

create table "my"."acquired_contractors"
(
    "contractor" integer not null primary key
);

create table "my"."ship_to_contractor"
(
    "ship"       integer not null primary key,
    "contractor" integer not null
);
/*

ALTER TABLE actions.offers
    ADD CONSTRAINT offers_contractor_key UNIQUE (contractor);

CREATE OR REPLACE FUNCTION update_offer_quantity()
    RETURNS TRIGGER AS
$$
BEGIN
    UPDATE actions.offers
    SET quantity = actions.offers.quantity + NEW.quantity
    WHERE actions.offers.contractor = NEW.contractor;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_quantity_trigger
    BEFORE INSERT
    ON actions.offers
    FOR EACH ROW
EXECUTE FUNCTION update_offer_quantity();*/


create or replace procedure acquireContractor(contractorId integer) as
$$
declare
    debugg boolean := true;
begin
    if 1 = 0 then raise notice 'acquireContractor %', contractorId; end if;
    insert into "my"."acquired_contractors" ("contractor") values (contractorId) on conflict do nothing;
end
$$ language plpgsql;

create or replace function getState(shipId integer) returns my.ship_state as
$$
declare
    result my.ship_state;
begin
    select "state" into result from "my"."ship_states" where "ship" = shipId;
    return result;
end;
$$ language plpgsql;

CREATE OR REPLACE PROCEDURE setState(shipId integer, stateInp my.ship_state, curTime double precision) AS
$$
declare
    debugg boolean := true;
BEGIN
    if 1 = 0 then raise notice 'setState % %', shipId, stateInp; end if;
    if stateInp is null then
        delete from "my"."ship_states" where "ship" = shipId;
        return;
    end if;
    INSERT INTO "my"."ship_states" ("ship", "state", "time")
    VALUES (shipId, stateInp, curTime)
    ON CONFLICT ("ship") DO UPDATE SET "state" = EXCLUDED."state", "time" = EXCLUDED."time";
END;
$$ LANGUAGE plpgsql;


CREATE INDEX offers_offer_index
    ON "my"."offers" ("offer");

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
declare
    debugg boolean := true;
BEGIN
    if 1 = 0 then raise notice 'setInt % %', keyInp, valueInp; end if;
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
declare
    debugg boolean := true;
BEGIN
    if 1 = 0 then raise notice 'setDouble % %', keyInp, valueInp; end if;
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
declare
    debugg boolean := true;
BEGIN
    if 1 = 0 then raise notice 'setText % %', keyInp, valueInp; end if;
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
declare
    debugg boolean := true;
begin
    if 1 = 0 then raise notice '[PLAYER %] MOVING SHIP % TO ISLAND %', player_id, ship_id, island_id; end if;
    insert into actions.ship_moves (ship, destination) values (ship_id, island_id);
end
$$ language plpgsql;

create function queryBestVendor(item_id integer, price_per_unitInp double precision)
    returns table
            (
                id             integer,
                type           world.contractor_type,
                island         integer,
                item           integer,
                quantity       double precision,
                price_per_unit double precision
            )
as
$$
begin
    return query select *
                 from world.contractors c
                 where c.type = 'vendor'
                   and c.item = item_id
                   and c.price_per_unit <= price_per_unitInp
                   and c.quantity >= 0.0
                 order by c.price_per_unit asc
                 limit 1;
end
$$ language plpgsql;

create function calculateProfit(vendor_id integer,
                                vendor_island integer,
                                vendor_qty double precision,
                                vendor_price_per_unit double precision,
                                customers_id integer,
                                customer_island integer,
                                customer_quantity double precision,
                                customer_price_per_unit double precision,
                                ship_id integer,
                                ship_capacity double precision,
                                ship_speed double precision)
    returns double precision
as
$$
declare
    debugg                   boolean          := true;
    shipInfo                 record;
    customerIsland           record;
    vendorIsland             record;
    shipIsland               record;
    shipToVendorDistance     double precision;
    vendorToCustomerDistance double precision;
    profitPerTime            double precision;
    totalTime                double precision;
    profit                   double precision;
    itemsQuantity            double precision;
    tmpInt                   integer;
    transfer_time_per_unit   double precision := 1.0;
begin
    totalTime := 0.0;

    -- get shipInfo
    select * into shipInfo from world.ships where id = ship_id;
    -- get distance between vendor and ship
    select * into vendorIsland from world.islands where id = vendor_island;
    select * into shipIsland from world.islands where id = (select island from world.parked_ships where ship = ship_id);
    select * into customerIsland from world.islands where id = customer_island;

    shipToVendorDistance := calcDistance(shipIsland.x, shipIsland.y, vendorIsland.x, vendorIsland.y);
    vendorToCustomerDistance := calcDistance(vendorIsland.x, vendorIsland.y, customerIsland.x, customerIsland.y);

    itemsQuantity := least(vendor_qty, customer_quantity, ship_capacity);
    profit := itemsQuantity * (customer_price_per_unit - vendor_price_per_unit);

    totalTime := shipToVendorDistance / ship_speed + vendorToCustomerDistance / ship_speed +
                 itemsQuantity * transfer_time_per_unit * 2;

    profitPerTime := profit / totalTime;
    return profitPerTime;
end;
$$ language plpgsql;

create type vendorBuyInfo as
(
    vendorId integer,
    quantity double precision
);

create type customerVendorVariant as
(
    customerId    integer,
    profit        double precision,
    qtyToSell     double precision,
    vendorsToPick vendorBuyInfo[]
);

create function calculateProfit2(
    customers_id integer,
    customer_island integer,
    customer_quantity double precision)
    returns customerVendorVariant
as
$$
declare
    customerInfo         record;
    vendorInfo           record;
    shipInfo             record;
    customerIsland       record;
    shipIsland           record;
    totalTime            double precision;
    profit               double precision;
    customerQtyRemaining double precision;
    vendorsToPick        vendorBuyInfo[] := Array []::vendorBuyInfo[];
    vendorQtyToGet       double precision;
begin
    totalTime := 0.0;

    -- get shipInfo
    select * into customerInfo from world.contractors where id = customers_id;

    -- get distance between vendor and ship
    -- select * into shipIsland from world.islands where id = (select island from world.parked_ships where ship = ship_id);
    select * into customerIsland from world.islands where id = customer_island;

    customerQtyRemaining := customer_quantity;
    profit := 0.0;
    for vendorInfo in
        select *
        from world.contractors ven
        where ven.type = 'vendor'
          and ven.item = customerInfo.item
          and ven.price_per_unit <= customerInfo.price_per_unit - 3.0
        order by ven.price_per_unit asc
        loop
            if customerQtyRemaining <= 0.0 then
                exit;
            end if;
            vendorQtyToGet := least(vendorInfo.quantity, customerQtyRemaining);
            if vendorQtyToGet <= 0.0 then
                continue;
            end if;

            customerQtyRemaining := customerQtyRemaining - vendorQtyToGet;

            profit := profit + vendorQtyToGet * (customerInfo.price_per_unit - vendorInfo.price_per_unit);

            vendorsToPick := vendorsToPick || ARRAY [row (vendorInfo.id, vendorQtyToGet)::vendorBuyInfo];
        end loop;

    return row (customers_id, profit, customer_quantity - customerQtyRemaining, vendorsToPick);

    --return query select customers_id, profit, vendorsToPick;
end;
$$ language plpgsql;

-- При расчете времени перемещения используется манхеттенское расстояние между текущим и целевым островом. При этом игровая карта является цикличной по обеим координатам.
create function calcDistance(x1 double precision, y1 double precision, x2 double precision, y2 double precision)
    returns double precision as
$$
declare
    debugg          boolean := true;
    xDiff           double precision;
    yDiff           double precision;
    map_size_actual double precision;
    result          double precision;
begin
    select map_size into map_size_actual from world.global;

    xDiff := abs(x1 - x2);
    yDiff := abs(y1 - y2);
    if xDiff > map_size_actual / 2 then
        xDiff := map_size_actual - xDiff;
    end if;
    if yDiff > map_size_actual / 2 then
        yDiff := map_size_actual - yDiff;
    end if;
    result := xDiff + yDiff;

    return result;
end
$$ language plpgsql;

CREATE PROCEDURE think(player_id INTEGER)
    LANGUAGE PLPGSQL AS
$$
declare
    new_var                  int;
    contractorId             int;
    currentTime              double precision;
    myMoney                  double precision;
    oppMoney                 double precision;
    ship                     record;
    shipWithState            record;
    contract                 record;
    contractor               record;
    contractorInfo           record;
    bestContractDraft        record;
    vendor                   record;
    vendorQtyToBuy           double precision;
    debugg                   boolean := true;
    tmpInt                   integer;
    shipContractor           integer;
    storageQty               double precision;
    customerStorageQty       double precision;
    cargoQty                 double precision;
    customerRichInfo         record;
    contractorToPick         record;
    profitInfoAdditional     record;
    customerInfo             record;
    vendorInfo               record;
    totalContractQty         double precision;
    totalStoredAtCustomerQty double precision;
    totalStoredAtOtherQty    double precision;
    -- totalStoredAtVendorQty double precision;
    totalParkedCargoQty      double precision;
    totalMovedCargoQty       double precision;
    totalShipCapacity        double precision;
    customerQty              double precision;
    customerPricePerUnit     double precision;
    vendorQty                double precision;
    vendorPricePerUnit       double precision;


BEGIN
    select game_time into currentTime from world.global;
    select money into myMoney from world.players where id = player_id;
    select money into oppMoney from world.players where id <> player_id order by id limit 1;
    if 1 = 0 then

        select coalesce(sum(con.quantity), 0.0)
        into totalContractQty
        from world.contracts con
        where con.player = player_id;

        select coalesce(sum(storage.quantity), 0.0)
        into totalStoredAtCustomerQty
        from world.contracts con,
             world.contractors contractors,
             world.storage storage
        where con.player = player_id
          and contractors.id = con.contractor
          and storage.island = contractors.island
          and storage.item = contractors.item;

        select coalesce(sum(storage.quantity), 0.0)
        into totalStoredAtOtherQty
        from world.contracts con,
             world.contractors contractors,
             world.storage storage
        where con.player = player_id
          and contractors.id = con.contractor
          and storage.island <> contractors.island
          and storage.item = contractors.item;

        select coalesce(sum(cargo.quantity), 0.0)
        into totalParkedCargoQty
        from world.cargo cargo,
             world.parked_ships parked_ships,
             world.ships ships
        where cargo.ship = ships.id
          and ships.player = player_id
          and parked_ships.ship = ships.id;

        select coalesce(sum(cargo.quantity), 0.0)
        into totalMovedCargoQty
        from world.cargo cargo,
             world.ships ships,
             world.moving_ships moving_ships
        where cargo.ship = ships.id
          and moving_ships.ship = ships.id
          and ships.player = player_id;

        select coalesce(sum(ships.capacity), 0.0)
        into totalShipCapacity
        from world.ships ships
        where ships.player = player_id;


        raise notice '[PLAYER %]      time: % and money: % opp: % myMoneyPerTime=% oppMoneyPerTime=% totalContractQty=% storedAtCustomerQty=% storedAtOtherQty=% parkedCargoQty=% movedCargoQty=% totalShipCapacity=% capacityUtilisation=%',
            player_id, currentTime, myMoney, oppMoney, myMoney / (currentTime + 0.0001), oppMoney / (currentTime + 0.0001),
            totalContractQty, totalStoredAtCustomerQty, totalStoredAtOtherQty, totalParkedCargoQty, totalMovedCargoQty, totalShipCapacity, (totalParkedCargoQty + totalMovedCargoQty) / totalShipCapacity;

        select count(*) into tmpInt from world.contracts con where con.player = player_id;
        raise notice '[PLAYER %] current_contracts count %', player_id, tmpInt;

        select count(*) into tmpInt from events.contract_completed contract_completed;
        raise notice '[PLAYER %] contract_completed count %', player_id, tmpInt;

        select count(*) into tmpInt from events.offer_rejected contractRejected;
        raise notice '[PLAYER %] offer_rejected count %', player_id, tmpInt;

        select count(*) into tmpInt from events.contract_started contractStarted;
        raise notice '[PLAYER %] contract_started count %', player_id, tmpInt;

        -- log top customer
        -- fill and log customerQty
        -- customerPricePerUnit
        -- vendorQty
        -- vendorPricePerUnit

        select con.quantity
        into customerQty
        from world.contractors con
        where con.id = 7;
        select con.price_per_unit
        into customerPricePerUnit
        from world.contractors con
        where con.id = 7;
        select con.quantity
        into vendorQty
        from world.contractors con
        where con.id = 91;
        select con.price_per_unit
        into vendorPricePerUnit
        from world.contractors con
        where con.id = 91;
        raise notice '[PLAYER %] customer 7, vendor 9 %;%;%;%', player_id, customerQty, customerPricePerUnit, vendorQty, vendorPricePerUnit;


    end if;

    -- handle initial setup of states
    if not exists (select * from my.ship_states) then
        if 1 = 0 then raise notice '[PLAYER %] setting initial state for ships', player_id; end if;
        for ship in select * from world.ships where player = player_id
            loop
                call setState(ship.id, 'idle', currentTime);
            end loop;
    end if;

    -- handle acquired customer contractors
    -- delete all acquired contractors
    delete from my.acquired_contractors;


    for contract in select * from world.contracts where player = player_id
        loop
            insert into my.acquired_contractors (contractor) values (contract.contractor);
        end loop;

    delete
    from my.ship_to_contractor
    where ship_to_contractor.contractor not in (select ac.contractor from my.acquired_contractors ac);

    -- print acquired contractors + contracts
    for contractorInfo in select ac.contractor, c.island, c.item, con.payment_sum, con.quantity
                          from my.acquired_contractors ac,
                               world.contractors c
                                   join world.contracts con on con.contractor = c.id and con.player = player_id
                          where ac.contractor = c.id
        loop
            /*   if 1 = 0 then
                   raise notice '[PLAYER %] acquired contractors info, contractor % island % item % payment sum % qty %',
                       player_id, contractorInfo.contractor, contractorInfo.island, contractorInfo.item,
                       contractorInfo.payment_sum, contractorInfo.quantity;
               end if;*/
        end loop;


    if 1 = 0 then
        raise notice '[PLAYER %] check best contracts acquired contractors count %', player_id, (select count(*) from my.acquired_contractors);
        --insert into actions.wait (until) values (currentTime + 1);

    end if;

    for customerRichInfo in
        select d.customerId,
               customerProfit,
               pg_typeof(d.customerProfit),
               (d.customerProfit).profit
        from (SELECT customers.id                                                         AS customerId,
                     calculateProfit2(customers.id, customers.island, customers.quantity) AS customerProfit
              FROM world.contractors customers
              WHERE customers.type = 'customer'
                AND customers.id NOT IN (SELECT ac.contractor FROM my.acquired_contractors ac)) d
        order by (d.customerProfit).profit desc
        limit 6 - coalesce((select count(*) from my.acquired_contractors), 0)
        loop
            raise notice '[PLAYER %] new contractor customerProfit %', player_id, to_json(customerRichInfo);

            insert into actions.offers (contractor, quantity)
            values (customerRichInfo.customerId, (customerRichInfo.customerProfit).qtyToSell);
        end loop;

    -- print current state of ships
    for shipWithState in
        select s.*,
               ss.state,
               coalesce(ps.island, ts.island)                                               island_coalesce,
               cargo.item,
               cargo.quantity,
               case
                   when ps.island is not null then 'parked'
                   when ts.island is not null then 'transferring'
                   when ms.start is not null then 'moving from ' || ms.start || ' to ' || ms.destination
                   else 'unknown!'
                   end as                                                                   game_state,
               (select stc.contractor from my.ship_to_contractor stc where stc.ship = s.id) shipContractor

        from world.ships s
                 left join my.ship_states ss on ss.ship = s.id
                 left join world.cargo cargo on s.id = cargo.ship
                 left join world.transferring_ships ts on s.id = ts.ship
                 left join world.parked_ships ps on s.id = ps.ship
                 left join world.moving_ships ms on s.id = ms.ship
        where s.player = player_id
        order by s.id
        loop
            if 1 = 0 then
                raise notice '[PLAYER %] ship % state % island % shipContractor % contains item % qty % game_state %',
                    player_id, shipWithState.id, shipWithState.state, shipWithState.island_coalesce, shipWithState.shipContractor,
                    shipWithState.item, shipWithState.quantity, shipWithState.game_state;
            end if;
        end loop;

    -- state machine
    for shipWithState in
        select s.*,
               ss.state,
               coalesce(ps.island, ts.island) island_coalesce,
               cargo.item,
               cargo.quantity,
               case
                   when ps.island is not null then 'parked'
                   when ts.island is not null then 'transferring'
                   when ms.start is not null then 'moving from ' || ms.start || ' to ' || ms.destination
                   else 'unknown!'
                   end as                     game_state

        from world.ships s
                 left join my.ship_states ss on ss.ship = s.id
                 left join world.cargo cargo on s.id = cargo.ship
                 left join world.transferring_ships ts on s.id = ts.ship
                 left join world.parked_ships ps on s.id = ps.ship
                 left join world.moving_ships ms on s.id = ms.ship
        where s.player = player_id
          and ps.island is not null
        order by s.capacity * s.speed asc
        loop
            if 1 = 0 then
                raise notice '[PLAYER %] handle parked states ship % state % island % contains item % qty % game_state %',
                    player_id, shipWithState.id, shipWithState.state, shipWithState.island_coalesce,
                    shipWithState.item, shipWithState.quantity, shipWithState.game_state;
            end if;


            shipContractor := null;

            select stc.contractor
            into shipContractor
            from my.ship_to_contractor stc
            where stc.ship = shipWithState.id;

            -- handle ship states
            if shipWithState.state = 'idle' then

                if shipContractor is null then
                    select ac.contractor,
                           (select count(*) from my.ship_to_contractor stc where stc.contractor = ac.contractor) count
                    into contractorToPick
                    from my.acquired_contractors ac
                    order by count asc
                    limit 1;

                    if contractorToPick is null then
                        if 1 = 0 then
                            raise notice '[PLAYER %] ship % no contractors to pick', player_id, shipWithState.id;
                        end if;
                    else
                        if 1 = 0 then
                            raise notice '[PLAYER %] ship % pick contractor %', player_id, shipWithState.id, contractorToPick.contractor;
                        end if;

                        insert into my.ship_to_contractor (ship, contractor)
                        values (shipWithState.id, contractorToPick.contractor);
                        shipContractor := contractorToPick.contractor;
                    end if;
                end if;

                if shipContractor is null then
                    -- wait for contractors
                    insert into actions.wait (until) values (currentTime + 1);
                else

                    select *
                    into customerInfo
                    from world.contracts contracts,
                         world.contractors customer
                    where contracts.player = player_id
                      and contracts.contractor = shipContractor
                      and customer.id = contracts.contractor;

                    -- check current island or go to vendor island, change state properly
                    -- TODO

                    select *
                    into vendorInfo
                    from world.contractors vendors
                    where vendors.type = 'vendor'
                      and vendors.item = customerInfo.item
                      and vendors.quantity > 10.0
                    order by vendors.price_per_unit asc
                    limit 1;

                    call setState(shipWithState.id, 'moving_to_load', currentTime);
                    insert into actions.wait (until) values (currentTime + 0.00000001);
                    if vendorInfo.island <> shipWithState.island_coalesce then
                        raise notice '[PLAYER %] ship % moving to vendor island %', player_id, shipWithState.id, vendorInfo.island;
                        call moveToTheNextIsland(player_id, shipWithState.id, vendorInfo.island);
                    end if;

                end if;


                /*
                if profitInfo.profitPerTime > 0.0 then
                    if 1 = 0 then
                        raise notice '[PLAYER %] ship % take contract to work %',
                            player_id, shipWithState.id, to_json(profitInfo);
                    end if;

                    insert into actions.offers (contractor, quantity)
                    values (profitInfo.vendorId, profitInfo.finalQuantity)
                    --           on conflict do nothing
                    returning id into tmpInt;

                    insert into actions.offers (contractor, quantity)
                    values (profitInfo.customerId, profitInfo.finalQuantity)
                    --         on conflict do nothing
                    returning id into tmpInt;

                    call setState(shipWithState.id, 'moving_to_load', currentTime);
                    call setInt('ship_contractor__' || shipWithState.id, profitInfo.customerId);
                    if profitInfo.vendorIsland <> shipWithState.island_coalesce then
                        call moveToTheNextIsland(player_id, shipWithState.id, profitInfo.vendorIsland);
                    end if;

                else
                    -- wait for offers
                    insert into actions.wait (until) values (currentTime + 0.001);
                end if;*/


            elseif shipWithState.state = 'moving_to_load' then
                -- ok so, items should be available at the island otherwise we should reacquire items

                contractorId := shipContractor;

                select * from world.contractors contractors where contractors.id = contractorId into contractor;
                select * from world.contracts contracts where contracts.contractor = contractorId into contract;

                if contract is null then
                    if 1 = 0 then
                        raise notice '[PLAYER %] WARN ship % contract % not found, back to idle', player_id, shipWithState.id, contractorId;
                    end if;
                    call setState(shipWithState.id, 'idle', currentTime);
                    insert into actions.wait (until) values (currentTime + 0.001);

                    -- if nothing found thats really bad
                else
                    -- check if enough in storage
                    select coalesce(sum(quantity), 0.0) from world.storage storage where item = contractor.item
                                                                                     and storage.island = shipWithState.island_coalesce into storageQty;


                    if storageQty < least(contract.quantity, 1) then

                        /*select vendors.id                                                          vendorId,
                               customers.id                                                        customerId,
                               vendors.island                                                      vendorIsland,
                               least(vendors.quantity, customers.quantity, shipWithState.capacity) finalQuantity,
                               calculateProfit(
                                       vendors.id,
                                       vendors.island,
                                       vendors.quantity,
                                       vendors.price_per_unit,
                                       customers.id,
                                       customers.island,
                                       customers.quantity,
                                       customers.price_per_unit,
                                       shipWithState.id,
                                       shipWithState.capacity,
                                       shipWithState.speed
                                   )                                                               profitPerTime

                        into customerRichInfo
                        from world.contractors vendors,
                             world.contractors customers
                        where vendors.item = customers.item
                          and vendors.type = 'vendor'
                          and customers.id = contractorId
                          and customers.type = 'customer'
                        --and customers.id not in (select ac.contractor from my.acquired_contractors ac)
                        order by profitPerTime desc
                        limit 1;*/

                        -- TODO find islands with big storage and not in the list of current customers + item
                        
                        select *
                        into vendorInfo
                        from world.contractors vendors
                        where vendors.type = 'vendor'
                          and vendors.item = contractor.item
                          and vendors.quantity > 10.0
                        order by vendors.price_per_unit asc
                        limit 1;

                        if vendorInfo is null then
                            if 1 = 0 then
                                raise notice '[PLAYER %] !ERROR! ship % contractorId % cannot find profitInfo + vendor, back to idle',
                                    player_id, shipWithState.id, contractorId;
                            end if;
                            call setState(shipWithState.id, 'idle', currentTime);
                            insert into actions.wait (until) values (currentTime + 0.1);

                        else
                            if 1 = 0 then
                                raise notice '[PLAYER %] ship % found vendor % island % do offer for %',
                                    player_id, shipWithState.id, vendorInfo.id, vendorInfo.island,
                                    least(vendorInfo.quantity, contract.quantity - storageQty);
                            end if;
                            insert into actions.offers (contractor, quantity)
                            values (vendorInfo.id, least(vendorInfo.quantity, contract.quantity - storageQty))
                            --   on conflict do nothing
                            returning id into tmpInt;
                            /*  update world.contractors
                              set quantity = quantity - profitInfo.finalQuantity
                              where id = profitInfo.vendorId;*/
                            --insert into my.offers (offer, time) values (tmpInt, currentTime);

                            call setState(shipWithState.id, 'moving_to_load', currentTime);
                            if vendorInfo.island <> shipWithState.island_coalesce then
                                call moveToTheNextIsland(player_id, shipWithState.id, vendorInfo.island);
                            end if;

                        end if;
                    else
                        -- ok, we have enough items in storage, lets load them
                        -- notice about loading stuff
                        if 1 = 0 then
                            raise notice '[PLAYER %] ship % loading % items from storage by payment_sum %',
                                player_id, shipWithState.id, storageQty, contract.payment_sum;
                        end if;
                        insert into actions.transfers (ship, item, quantity, direction)
                        values (shipWithState.id,
                                contractor.item,
                                storageQty,
                                'load');
                        call setState(shipWithState.id, 'wait_for_load_finish', currentTime);
                    end if;
                end if;

            elseif shipWithState.state = 'wait_for_load_finish' then
                contractorId := shipContractor;

                select * from world.contractors contractors where contractors.id = contractorId into contractor;
                select * from world.contracts contracts where contracts.contractor = contractorId into contract;

                -- check if enough qty in cargo
                select coalesce(sum(quantity), 0.0)
                from world.cargo cargo
                where cargo.ship = shipWithState.id
                  and cargo.item = contractor.item
                into cargoQty;

                if cargoQty < 0.001 then
                    if 1 = 0 then
                        raise notice '[PLAYER %] WARN ship % not enough items in cargo, back to ''moving_to_load''', player_id, shipWithState.id;
                    end if;
                    call setState(shipWithState.id, 'moving_to_load', currentTime);
                    insert into actions.wait (until) values (currentTime + 0.1);
                else
                    -- ok, we have enough items in cargo, lets go to destination
                    call moveToTheNextIsland(player_id, shipWithState.id, contractor.island);
                    call setState(shipWithState.id, 'moving_to_unload', currentTime);
                end if;

            elseif shipWithState.state = 'moving_to_unload' then
                contractorId := shipContractor;

                select * from world.contractors contractors where contractors.id = contractorId into contractor;
                select * from world.contracts contracts where contracts.contractor = contractorId into contract;

                insert into actions.transfers (ship, item, quantity, direction)
                values (shipWithState.id,
                        shipWithState.item,
                        shipWithState.capacity,
                        'unload');

                call setState(shipWithState.id, 'idle', currentTime);
            else
                if 1 = 0 then
                    raise notice '[PLAYER %] ship % unknown and unhandled state %', player_id, shipWithState.id, shipWithState.state;
                end if;
            end if;

        end loop;
    insert into actions.wait (until) values (currentTime + 5);
END
$$;