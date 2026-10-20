# Security regression tests

Each `test_<id>_<what>.py` here is the regression test for one security finding, named by the finding's audit ID.
Its module docstring says what the attack was, how the test rebuilds it, and what the fix does. This file maps each
ID to its test modules, the code the test guards and the commits that fixed it, grouped by subsystem. To find every
test for a module, search this file for its name (for example `keepers.py` or `server.py`).

Where the IDs come from:
- **Earlier audits** (H, M, L, N, NEW, I, R and T): the October 2026 audits, fixed before 0.1.4. A suffix `-R`,
  `-R1` or `-R2` marks a residual found when the fix was re-tested, and "follow-up" a later fix to the same
  finding. Their write-ups weren't kept; the test docstrings are the record. The fixes were made on `fix/*` and
  `sec/*` branches, merged through `security/audit-2026-10` and released in 0.1.4. The commits named are those
  branches' commits, all in the history of the `v0.1.4` tag.
- **0.1.5 audit** (CONT, DHT, API, FEAT, NET and SUP): `0.1.5-audit.md`, fixed in 0.1.6 (`0.1.6-plan.md`).
- **No ID:** tests from an earlier review or from the security tooling, listed under the subsystem they guard.

Test modules are in this directory unless a path says otherwise. A commit marked "(test)" only added or updated
the test; the others changed the code. "Pages" are the NomadNet companion pages in `companion-nomadnet-sites/`.

Run them with `python3 -m unittest discover -s tests/security -p "test_*.py"`. Those needing real RNS
(`test_h1_real_rns.py`, `test_m1_real_rns.py` and the real-daemon part of `test_h2_display_page_pins.py`) run only
with `RNFS_E2E=1`, and `test_fuzz_parsers.py` only where hypothesis is installed; see CONTRIBUTING.md.

Shared fakes and helpers (not test modules): `_content_helpers.py` (a DAG declaring 1 byte but holding megabytes,
and two offline nodes counting what B downloads), `_server_helpers.py` (fake links and a `RequestServer` on a
FakeClock), `_fetchr2_helpers.py` (padded and chained DAGs, and a byte-range read's allowance),
`_dhtr2b_helpers.py` (SybilNet and the keeper scenario), `_pages_helpers.py` (the pages' own code against an offline node
and a fake CLI), and `_h1_real_helpers.py` and `_m1_real_helpers.py` (the real-RNS hub and attacker processes).

## Content, storage and fetching

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| L3 | `test_l3_zero_size_children.py`, `test_dag_caps.py` | `rnfs/fetcher.py` (`dag_bound`, `node_credit`, `fetch_dag`) | `211c470`, `4bdd14a` (test) |
| N1 | `test_n1_user_pin_unbounded.py`, `test_n1_display_page_unbounded.py`, `test_dag_caps.py` | `rnfs/fetcher.py` (`fetch_dag`), `rnfs/node.py` (`pin_add`), display-mu-file page | `211c470`, `6b23f35` (page) |
| N1-R1 | `test_n1_read_range_cap.py` | `rnfs/fetcher.py` (`Fetch.read_range`) | `2290b66` |
| N1-R2 | `test_n1_listing_chains.py` | `rnfs/fetcher.py` (`fetch_listing`) | `2290b66` |
| N1-R3 | `test_n1_entry_credit.py` | `rnfs/fetcher.py` (`node_credit`) | `2290b66` |
| L8 | `test_l8_folder_swap.py`, `test_l8_path_walk_checks.py` | `rnfs/content/builder.py`, `rnfs/content/shared_folder.py`, `rnfs/fsutil.py` | `b77bf1a`, `b18fb02` (test) |
| L8-R2 | `test_l8r2_hard_links.py` | `rnfs/content/shared_folder.py`, `rnfs/content/builder.py` | `1d8e099` |
| L9 | `test_l9_get_free_space.py`, `test_l9_free_space_unknown.py` | `rnfs/node.py` (`_check_free_space`), `rnfs/node_content.py`, `rnfs/content/storage.py` | `7640d0b`; failing closed: `383f770` |
| NEW-5 | `test_new5_storage_lock_walks.py` | `rnfs/content/storage.py` (`evict`, `gc`) | `6f6b36e` |
| CONT-1 | `test_cont1_deny_hash_amplification.py` | `rnfs/denylist.py`, `rnfs/content/walker.py`, `rnfs/node_content.py`, `rnfs/node.py` (`_check_file`) | `5454d5d` |
| CONT-2 | `test_cont2_write_out_swap.py` | `rnfs/content/walker.py` (`write_out`), `rnfs/node.py` (`get`), `rnfs/winacl.py` | `04b3e2e` |
| CONT-3 | `test_cont3_shared_folder_target.py` | `rnfs/content/shared_folder.py` (`SharedFolder.scan`), `rnfs/node.py` | `3fb49ae`, `8d30c61` |
| CONT-4 / API-6 | `test_cont4_denied_hash_notice.py` | `rnfs/node.py` (`_check_file`); documented in specification section 8 | `c61a324` |

## DHT, providers and names

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| M3 | `test_m3_keeper_discovery.py` | `rnfs/features/keepers.py`, `rnfs/dht/lookup.py`, `rnfs/dht/routing.py`, `rnfs/fetcher.py` | `424b20b` |
| M3-R1 | `test_m3r1_keeper_standing.py` | `rnfs/features/keepers.py` (`KeepersFeature.ask_keepers`), `rnfs/dht/lookup.py`, `rnfs/dht/routing.py` | `c83ceab`, `e2e78c2` (test) |
| M3-R2 | `test_m3r2_keeper_unknown_budget.py` | `rnfs/features/keepers.py`, `rnfs/dht/lookup.py` | `c83ceab` |
| M4 | `test_m4_provider_admission.py` (rebuilt for 0.2.0's record budget and tiers) | `rnfs/dht/providers.py` (`ProviderStore`: `_reclaim`), `rnfs/network/protocol.py`, `rnfs/node.py` | `641b9b2` |
| M4-R | `test_m4r_newcomer_pool.py` (rebuilt for 0.2.0: the Newcomer tier) | `rnfs/dht/providers.py` (`_reclaim`, `_largest`, the standing kept by `_remove`) | `2f72d80` |
| NEW-1 | `test_new1_publisher_withdraw_race.py` | `rnfs/publisher.py` (`publish_due`, `refresh`) | `c3b4d45` |
| NEW-4 | `test_new4_private_mode_records.py` | `rnfs/dht/providers.py`, `rnfs/node.py` (`NodeBackend.find`), `rnfs/network/links.py` (`LinkPool`) | `1ffc8eb` |
| NEW-6 | `test_new6_publisher_crash_cut_round.py`; remote-pin half: `test_new6_remote_pins_restart.py`; follow-up: `test_new6_remote_pins_bad_entry.py` | `rnfs/publisher.py`; `rnfs/node.py` (`_load_remote_pins`, `_resume_remote_pins`), `rnfs/statedb.py` | `c3b4d45`; `719c1bc`; `49df3c9` |
| NEW-8 | `test_new8_provide_at_limit.py` (rebuilt for 0.2.0: a full budget) | `rnfs/dht/providers.py` (`ProviderStore.provide`, `_store`) | `d74dcc5` |
| L4 | `test_l4_name_rollback.py` | `rnfs/names.py` (`NameManager`, the name table) | `97b2537` |
| L4-R | `test_l4r_name_floor.py` | `rnfs/names.py` (the gateway's name floor) | `a9c5640` |
| NEW-2 | `test_new2_name_publish_crash.py`, `test_new2_follow_lxmf_pins.py` | `rnfs/names.py` (`NameManager.publish`), `rnfs/node.py`, `rnfs/features/follow.py`, `rnfs/content/storage.py` | `8ecad5b`, `ef3cc9d` |
| R-2 | `test_r2_name_key_unreadable.py` | `rnfs/names.py` (`release_gone`, `reconcile_pins`) | `56ed5ac` |
| T1 | `test_t1_parse_name_whitespace.py`; residual: `test_t1_parse_hex_id_whitespace.py` | `rnfs/names.py` (`parse_name`); `rnfs/api.py` (`parse_hex_id`) | `63ebae9`; `bb23b64` |
| DHT-1 | `test_dht1_keeper_query_budget.py` | `rnfs/features/keepers.py`, `rnfs/dht/lookup.py` | `73c8295` |
| DHT-2 | `test_dht2_replayed_announce.py`; also `tests/test_names.py` | `rnfs/names.py` (`RnsNetwork.recalled_emission`) | `c31372b` |
| DHT-4 | `test_dht4_private_name_announces.py`; also `tests/test_public.py` | `rnfs/names.py` (`NameManager.publish`), `rnfs/cli.py` | `509c056` |
| DHT-5 | `test_dht5_own_provider_record.py`; also `tests/test_providers.py` | `rnfs/dht/providers.py` (`_key_room`, `_reclaim`, the own tier) | `4b72ea7` |
| DHT-6 | none: documented only (the cost of grinding node IDs, specification section 10) | none | `4441878` (docs) |

## Network and the `/blocks` server

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| H1 | `test_h1_response_slots.py`, `test_h1_real_rns.py` (`RNFS_E2E`), `test_rns_compat_security.py` | `rnfs/network/server.py` (`ResponsePools`, `RequestServer`), `rnfs/config.py` | `dbd1144`, `7b9f6b9` and `c8d09ad` (test) |
| H1 (follow-up) | `test_h1_regressions.py` | `rnfs/network/server.py` (`link_path`, the grace and fairness rules), `rnfs/config.py` | `126063d`, `c6f4a74`, `86c4475`, `90a2952` (test) |
| M1 | `test_m1_half_open_links.py`, `test_m1_real_rns.py` (`RNFS_E2E`), `test_rns_compat_security.py` | `rnfs/network/guards.py` (`limit_half_open`) | `09c6324` |
| M2 | `test_m2_upload_budget.py` | `rnfs/network/server.py` (upload_budget), `rnfs/mesh.py` | `0726417` |
| M2 (follow-up) | `test_m2_commitment_bound.py` | `rnfs/network/server.py` | `1af981f` |
| L1 | `test_l1_response_precheck.py`, `test_rns_compat_security.py` | `rnfs/network/guards.py`, `rnfs/network/protocol.py` (`response_refusal`) | `73041a7` |
| L2 | `test_l2_blocks_request_limits.py` | `rnfs/network/protocol.py` (a repeated CID), `rnfs/network/server.py` (the budget with no rate estimate) | `80b950d`, `a98b985` |
| L17 | `test_l17_rejected_responses.py` (L17a), `test_l17_request_rate.py` (L17b), `test_rns_compat_security.py` | `rnfs/network/server.py` (the cooldown; `RequestServer`'s per-link request rate) | `42aadca` (L17a), `6c79d4c` (L17b) |
| NEW-3 | `test_new3_remote_pin_wakeup.py` | `rnfs/node.py` (`_remote_pin_worker`) | `719c1bc` |
| NET-1 | `test_net1_cancelled_responses.py`, `test_l17_rejected_responses.py` | `rnfs/network/server.py` (`NO_PROGRESS_FRACTION`) | `0ec517b` |
| NET-2 | `test_net2_single_packet_pacing.py`; also `tests/test_pacing.py`, `tests/test_server.py` | `rnfs/network/server.py` (pacing under `max_upload_rate`) | `c0ec333` |
| NET-3 | `test_net3_outgoing_requests.py`, `test_l1_response_precheck.py` | `rnfs/network/guards.py` (`guarded_receive`) | `81afb98` |
| NET-4 | `test_net4_dripping_provider.py`; also `tests/test_fetcher.py` (`ReadAheadLinkTest`) | `rnfs/fetcher.py` (end-game duplication of a batch in flight past `deadline_slack`), `rnfs/network/client.py` and `rnfs/network/links.py` (`close_unless_used`) | `afa3820`, `ac9ed4f` |
| NET-5 | `test_net5_proof_wait.py` | `rnfs/network/server.py` (`RequestServer._proof_wait`) | `db27b8b` |
| none (NomadNet links) | `test_nomadnet_link_bounds.py`; also `tests/test_nomadnet.py`, `tests/test_nomadnet_e2e.py` (`RNFS_E2E`) | `rnfs/network/nomadnet.py` (`Transfer`, `segment_refusal`), `rnfs/network/guards.py` (`mark_nomadnet`, the page check), `rnfs/network/protocol.py` (`page_response_refusal`, `metadata_refusal`) | `4e9c97b`, `2b2beb4` (built with NomadNet add, 0.2.0) |
| none (RNS internals) | `test_rns_compat_security.py` | the RNS attributes `rnfs/network/server.py` and `rnfs/network/guards.py` rely on (H1, M1, L1, L17) | `dbd1144`, `09c6324`, `73041a7`, `42aadca`, `90a2952` (test) |

## Local API, client and gateway

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| I1 | `test_i1_507_message.py` | `rnfs/gateway.py` (the 507 message), `rnfs/content/storage.py` (`StorageFull`) | `68dcb8f` |
| L6 | `test_l6_gateway_storage_oracle.py` | `rnfs/gateway.py`, `rnfs/gateway_formats.py` (`?format=`) | `a678443`, `3265621`, `4a765d4` (documented limit) |
| NEW-7 | `test_new7_api_status_open.py` | `rnfs/api.py` (`GET /api/status`), `rnfs/cli.py` | `863f0b7` |
| none (CORS) | `test_gateway_cors.py` | `rnfs/gateway.py`, `rnfs/gateway_formats.py` | `d77be66` |
| none (site hosts) | `test_gateway_site_hosts.py`; also `tests/test_gateway_sites.py` | `rnfs/gateway.py` (`parse_host`, `serve_site`, the site CSP), `rnfs/content/cid.py` (`from_base36`) | `1883af2` (built with the subdomain gateway, 0.2.0) |
| none (headers) | `test_response_headers.py` | `rnfs/gateway.py` (`?filename=`), `rnfs/api.py` (`no-store`, `nosniff`) | `bfe9bc7` (test; pins existing behaviour) |
| API-2 | `test_api2_proof_first.py` | `rnfs/apiclient.py` (`verify_daemon`, `PROOF_TIMEOUT`) | `dda4603` |
| API-3 | none: documented only (the proof keep-open, specification section 11) | none | `feba0f0` (docs) |
| API-4 | `test_api4_old_daemon_fallback.py`; also `tests/test_apiclient.py` | `rnfs/apiclient.py` (the pre-0.1.5 reconnect, dropped in 0.2.0: such daemons are refused in `check_daemon`) | `b565ee8` |
| API-5 | `test_api5_head_deny.py` | `rnfs/node_content.py` (`stored_head`), `rnfs/gateway.py`, `rnfs/node.py` | `cb5c93f` |
| API-7 | `test_api7_missing_fetch_metadata.py` | `rnfs/gateway.py` (`limited`, the User-Agent rule) | `0d010b1` |

## Features: follow

The LXMF feature's findings (L12, L12-R, L13, L13-R, FEAT-1 to FEAT-5 and the auto-fetch fairness fix) and their
regression tests left rnfs with the feature in 0.2.0, when LXMF messaging became an app of its own; R-1 and NEW-2
keep their follow tests.

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| L5 | `test_l5_follow_cap.py` | `rnfs/features/follow.py`, `rnfs/fetcher.py` (`dag_bound`) | `211c470` |
| R-1 | `test_r1_reconcile_damaged_state.py` | `rnfs/features/follow.py` (the start-up reconcile) | `0e350ae` |
| DHT-3 | `test_dht3_follow_quota.py`, `test_l5_follow_cap.py` | `rnfs/features/follow.py` (`follow_quota`), `rnfs/config.py` | `63a0157` |
| FEAT-6 | `test_feat6_printable_label.py` | `rnfs/fsutil.py` (`printable_label`) | `0c97eb7` |
| FEAT-7 | `test_feat7_example_and_client_escaping.py` | `examples/rnfs_share.py`, `rnfs/remote.py` (`Client._report`) | `71d7cb2` |

## Companion pages

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| H2 | `test_h2_display_page_pins.py` (part `RNFS_E2E`) | display-mu-file page | `6b23f35` |
| H2-R | `test_h2r_display_page_private_and_rate.py` | display-mu-file page (private mode, `VISITOR_LIMIT`) | `3707b53` |
| L10 | `test_l10_index_page_injection.py` | both pages' `escape()` | `9098381`, `6b23f35` |
| L11 | `test_l11_display_page_micron.py` | display-mu-file page (`plain_text`, the allowlist) | `6b23f35` |
| L14 | `test_l14_page_local_paths.py` | both pages (`log`, which writes page.log; display-mu-file's `fail`) | `6b23f35`, `9098381` |
| M6 | `test_m6_index_page_names.py`, `test_m6_pin_ls_sealed.py`; follow-up: `test_m6_index_page_full_run.py` | file-index-node page (`entry`, `shown`, `is_sealed`); `rnfs/node.py` (`pin_ls`), `rnfs/cli.py` | `9098381`, `9544fd6`; `5092f80` |
| FEAT-8 | `test_feat8_index_page_cache.py` | file-index-node page (`take_lock`, `cached`) | `be571fc` |

N1's page half is in the content table.

## Platform: files, permissions and Windows

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| L7 | `test_l7_windows_acl.py` | `rnfs/winacl.py`, `rnfs/fsutil.py`, `rnfs/network/identity.py`, `rnfs/names.py`, `rnfs/node.py`, `rnfs/cli.py` | `1d0b0e6`, `2be6acf` |
| L7-R1 | `test_l7r1_windows_dir_creation.py` | `rnfs/winacl.py` (`create_private_dir`), `rnfs/config.py`, `rnfs/fsutil.py`, `rnfs/node.py` | `4383bfa` |
| L7-R2 | `test_l7r2_junction_root.py` | `rnfs/fsutil.py`, `rnfs/node.py` (the root check) | `4383bfa` |
| I2 | `test_i2_identity_symlink.py`; packaging half: `test_i2_packaging_hardening.py` | `rnfs/network/identity.py` (`load_or_create_identity`); `.github/workflows/release.yml`, `contrib/rnfs.service`, `install.sh` | `ea88d13`; `cbf257e` |
| I2-R1 | `test_i2r1_identity_file.py` | `rnfs/network/identity.py` (`IdentityFileError`) | `2db7830` |
| API-1 | `test_api1_settings_file.py`, `test_api1_subdirs.py` | `rnfs/config.py`, `rnfs/fsutil.py`, `rnfs/node.py`, `rnfs/embedded.py`, `rnfs/cli.py`, `rnfs/apiclient.py` | `939ecae` |

## Packaging and release

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| L15 | `test_l15_install_release_check.py` | `install.sh` (`download`, `verify_release`, `fetch_release`) | `e2b5686` |
| L16 | `test_l16_release_container.py` | `.github/workflows/release.yml` (the Debian 12 smoke test) | `1f3cb70` |
| SUP-1 | `tests/test_packaging.py` (`test_flatpak_builds_skip_the_actions_cache`) | `.github/workflows/release.yml` | `6fd0cc7` |
| SUP-2 | `test_l15_install_release_check.py`; also `tests/test_packaging.py` (`test_release_wheel_is_of_the_tag`, `test_provenance_names_the_tag`) | `install.sh` | `67bfafe` |
| SUP-3 | `test_l15_install_release_check.py` | `install.sh` | `c64ebf5` |
| SUP-4 | none: the PyPI name is the maintainer's to claim; the code side is a README note | `README.md` | `4ac4b89` (docs) |
| SUP-5 | `tests/test_packaging.py` (`test_flatpak_sandbox_is_narrow`) | `packaging/flatpak/io.github.afit21.rnfs.yml` | `021c7c2` |
| SUP-6 | `tests/test_packaging.py` (`test_unit_hardening_is_an_opt_in_drop_in`, `test_deb_postinst_python_is_isolated`, `test_plain_http_source_warns`) | `contrib/rnfs.service`, `contrib/rnfs.service.d/hardening.conf`, `packaging/build-deb.sh`, `install.sh` | `c00f8d7` |

## Across subsystems

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| none (fuzzing) | `test_fuzz_parsers.py` (needs hypothesis) | the strict parsers of untrusted bytes: `rnfs/content/cid.py`, `rnfs/content/dag.py`, `rnfs/content/storage.py`, `rnfs/names.py`, `rnfs/network/`, `rnfs/uris.py`, `rnfs/gateway.py` | `077f33a` (test); found T1 |

## 0.2.0 review (REV)

A read-only review of 0.2.0 against remote and local attackers, fixed before 0.2.0 was released; the findings are
the roadmap's "Security fixes from the 0.2.0 review".

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| REV4 | `test_rev4_forced_partition.py` | `rnfs/dht/local.py` (`PartitionMode.failed`, `probe_target`), `rnfs/node.py` (`_knows_server`, `_run_local`) | "Partition mode can't be forced from outside (REV4)" |
| REV7 | `test_rev7_firstseen_seniors.py` | `rnfs/dht/firstseen.py` (`FirstSeenLog._evict`, `_victim`), `rnfs/node.py` (the log's settings) | "The announce first-seen log keeps its senior servers (REV7)" |
