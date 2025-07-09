#!/usr/bin/env python3
"""Fix the export_by_token_ranges method to handle consistency level properly."""

# Here's the corrected version of the export_by_token_ranges method

corrected_code = """
        # Stream results from each range
        for split in splits:
            # Check if this is a wraparound range
            if split.end < split.start:
                # Wraparound range needs to be split into two queries
                # First part: from start to MAX_TOKEN
                if consistency_level is not None:
                    async with await self.session.execute_stream(
                        prepared_stmts["select_wraparound_gt"],
                        (split.start,),
                        consistency_level=consistency_level
                    ) as result:
                        async for row in result:
                            stats.rows_processed += 1
                            yield row
                else:
                    async with await self.session.execute_stream(
                        prepared_stmts["select_wraparound_gt"],
                        (split.start,)
                    ) as result:
                        async for row in result:
                            stats.rows_processed += 1
                            yield row

                # Second part: from MIN_TOKEN to end
                if consistency_level is not None:
                    async with await self.session.execute_stream(
                        prepared_stmts["select_wraparound_lte"],
                        (split.end,),
                        consistency_level=consistency_level
                    ) as result:
                        async for row in result:
                            stats.rows_processed += 1
                            yield row
                else:
                    async with await self.session.execute_stream(
                        prepared_stmts["select_wraparound_lte"],
                        (split.end,)
                    ) as result:
                        async for row in result:
                            stats.rows_processed += 1
                            yield row
            else:
                # Normal range - use prepared statement
                if consistency_level is not None:
                    async with await self.session.execute_stream(
                        prepared_stmts["select_range"],
                        (split.start, split.end),
                        consistency_level=consistency_level
                    ) as result:
                        async for row in result:
                            stats.rows_processed += 1
                            yield row
                else:
                    async with await self.session.execute_stream(
                        prepared_stmts["select_range"],
                        (split.start, split.end)
                    ) as result:
                        async for row in result:
                            stats.rows_processed += 1
                            yield row

            stats.ranges_completed += 1

            if progress_callback:
                progress_callback(stats)

        stats.end_time = time.time()
"""

print(corrected_code)
